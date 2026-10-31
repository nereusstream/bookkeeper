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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicHandoffPaths.Identity;
import org.apache.bookkeeper.common.util.nativeio.NativeIO;
import org.apache.bookkeeper.common.util.nativeio.NativeIOBootstrap;
import org.apache.bookkeeper.common.util.nativeio.NativeIOImpl;

/** Owned fixed trust I/O. Parent-private reads do not verify a running issuer or issue startup authority. */
final class SegmentClassicLaunchTrust {
    private static final long MAGIC = 0x5557434c52303031L; // UWCLR001
    static final int MAX_RECIPE_BYTES = 4096;
    private static final byte[] RECIPE_DOMAIN = "BookKeeper/ControlledClassicRecipe/v1\0"
            .getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_LOADING_DIRECTORY_ENTRIES = 262144;
    private static final int MAX_LOADING_DIRECTORY_NAME_BYTES = 64 * 1024 * 1024;

    record Recipe(String recipeId, String deployment, String keyName, SegmentClassicHandoff.Digest signerKeyId,
                  SegmentClassicHandoff.Digest program, SegmentClassicHandoff.Digest configuration,
                  SegmentClassicHandoff.Digest originalScope, long oldUid, long primaryGid, List<Long> groups) {
        Recipe {
            SegmentClassicLaunchCodec.recipeId(recipeId);
            SegmentClassicLaunchCodec.recipeId(keyName);
            if (deployment == null || deployment.isBlank() || deployment.indexOf(0) >= 0
                    || !StandardCharsets.UTF_8.newEncoder().canEncode(deployment)
                    || deployment.getBytes(StandardCharsets.UTF_8).length > 1024) {
                throw new IllegalArgumentException("Invalid fixed deployment identity");
            }
            Objects.requireNonNull(signerKeyId, "signerKeyId");
            Objects.requireNonNull(program, "program");
            Objects.requireNonNull(configuration, "configuration");
            Objects.requireNonNull(originalScope, "originalScope");
            groups = List.copyOf(groups);
            if (oldUid <= 0 || oldUid > 0xffff_ffffL || primaryGid <= 0 || primaryGid > 0xffff_ffffL
                    || groups.size() > 64) {
                throw new IllegalArgumentException("Bounded nonprivileged Classic credentials required");
            }
            long previous = 0;
            for (long group : groups) {
                if (group <= previous || group > 0xffff_ffffL) {
                    throw new IllegalArgumentException("Expected supplementary groups must be unique and ordered");
                }
                previous = group;
            }
        }
    }

    /** Parsed values alone never prove where they were read or qualify a process. */
    record Parsed(Recipe recipe, SegmentClassicHandoff.Digest recipeIdentity, PublicKey publicKey) { }

    /** Observed bytes/objects only; neither installed deployment approval nor live Linux qualification. */
    record LinuxFileObservation(Identity identity, List<Identity> parents, SegmentClassicLinuxElf.Image elf,
                                SegmentClassicLinuxCache.Image cache,
                                SegmentClassicLinuxConfiguration.Image configuration) {
        LinuxFileObservation {
            parents = List.copyOf(parents);
        }
    }

    enum LinuxPathKind { ALIAS, DIRECTORY, CONFIGURATION, INTERPRETER }

    /** Bounded path facts only. The chain digest compares two observations, never startup authority. */
    record LinuxPathObservation(LinuxPathKind kind, Path entry, Path resolved, Path absentAt, Identity identity,
                                SegmentClassicHandoff.Digest chain) { }

    /** Complete observed names only, not approved library candidates or a loader selection. */
    record LinuxDirectoryObservation(LinuxPathObservation path, List<String> members) {
        LinuxDirectoryObservation {
            members = List.copyOf(members);
        }
    }

    private static final class LoadingDirectoryBudget {
        int entries;
        int nameBytes;

        void retain(String name) throws IOException {
            int bytes = name.getBytes(StandardCharsets.UTF_8).length;
            if (++entries > MAX_LOADING_DIRECTORY_ENTRIES || bytes > MAX_LOADING_DIRECTORY_NAME_BYTES - nameBytes) {
                throw new IOException("Loading directory observations exceed their fixed total name capacity");
            }
            nameBytes += bytes;
        }
    }

    /** Only the fixed-root UID-0 loader can create this provenance; a fixture cannot promote parsed values. */
    static final class Installed {
        private final SegmentClassicLaunchTrust owner;

        private Installed(SegmentClassicLaunchTrust owner) {
            this.owner = owner;
        }

        Parsed material() throws IOException {
            synchronized (owner) {
                owner.requireReady();
                return owner.parsed;
            }
        }
    }

    record Drain(boolean drained, Throwable initializationFailure, SegmentOwnedFiles.Result files,
                 int retainedAuxiliaryReads, Throwable auxiliaryCloseFailure, Throwable programFailure,
                 Throwable configurationFailure, Throwable launcherFailure) { }

    private static final class AuxiliaryRead {
        AutoCloseable resource;
        boolean openStarted;
        boolean openReturned;
        boolean closeAttempted;
        Throwable failure;
    }

    private final Path root;
    private final String recipeId;
    private final long ownerUid;
    private final boolean production;
    private final SegmentClassicLaunchPreload preload;
    private final NativeIO nativeIo = new NativeIOImpl();
    private final SegmentOwnedFiles files = new SegmentOwnedFiles();
    private boolean attempted;
    private boolean closing;
    private Thread initializingThread;
    private Throwable initializationFailure;
    private Parsed parsed;
    private Installed installed;
    private boolean programAttempted;
    private Throwable programFailure;
    private boolean configurationAttempted;
    private Throwable configurationFailure;
    private boolean launcherDescriptorAttempted;
    private boolean launcherKeyAttempted;
    private Throwable launcherFailure;
    private Throwable auxiliaryCloseFailure;
    private final List<AuxiliaryRead> auxiliaryReads = new ArrayList<>();

    private SegmentClassicLaunchTrust(Path root, String recipeId, long ownerUid, boolean production,
                                      SegmentClassicLaunchPreload preload) {
        SegmentClassicLaunchCodec.recipeId(recipeId);
        if (!root.isAbsolute() || !root.normalize().equals(root) || ownerUid < 0 || ownerUid > 0xffff_ffffL) {
            throw new IllegalArgumentException("Invalid public trust material root");
        }
        this.root = root;
        this.recipeId = recipeId;
        this.ownerUid = ownerUid;
        this.production = production;
        this.preload = preload;
    }

    /** No property, environment or caller path override. Preparation performs no I/O. */
    static SegmentClassicLaunchTrust production(SegmentClassicLaunchPreload preload) throws IOException {
        preload.adopt(true);
        return new SegmentClassicLaunchTrust(preload.root(), preload.recipeId(), 0, true, preload);
    }

    /** File/ACL fixture only: initialize can return Parsed, never Installed, irrespective of the UID argument. */
    static SegmentClassicLaunchTrust forMaterialIoTests(Path root, String recipeId, long ownerUid) {
        return new SegmentClassicLaunchTrust(root, recipeId, ownerUid, false, null);
    }

    /** Same original early reads in a file fixture, with no Installed provenance. The preload remains borrowed. */
    static SegmentClassicLaunchTrust forPreloadedIoTests(SegmentClassicLaunchPreload preload) throws IOException {
        preload.adopt(false);
        return new SegmentClassicLaunchTrust(preload.root(), preload.recipeId(), preload.uid(), false, preload);
    }

    synchronized void initialize() throws IOException {
        if (attempted || closing) {
            throw new IOException("Classic trust material initialization is one-use");
        }
        attempted = true;
        initializingThread = Thread.currentThread();
        try {
            if (preload != null) {
                preload.requireActive();
            }
            var before = chain(root);
            requireRoot();
            byte[] bytes = read(root.resolve(recipeId + ".recipe"), 16, MAX_RECIPE_BYTES);
            Recipe recipe = decode(bytes);
            if (!recipe.recipeId().equals(recipeId)) {
                throw new IOException("Installed recipe does not match its selected bounded ID");
            }
            byte[] raw = read(root.resolve(recipe.keyName() + ".ed25519"), 32, 32);
            byte[] spki = ByteBuffer.allocate(44).put(HexFormat.of().parseHex("302a300506032b6570032100"))
                    .put(raw).array();
            PublicKey key = KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(spki));
            requireRoot();
            if (!before.equals(chain(root))) {
                throw new IOException("Installed trust parent chain changed during the read");
            }
            files.requireHealthy();
            parsed = new Parsed(recipe, SegmentClassicHandoff.digest(RECIPE_DOMAIN, bytes), key);
            if (production) {
                installed = new Installed(this);
            }
        } catch (GeneralSecurityException badKey) {
            initializationFailure = new IOException("Invalid fixed Ed25519 public key", badKey);
            throw (IOException) initializationFailure;
        } catch (IOException | RuntimeException | Error failure) {
            initializationFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    synchronized Parsed parsed() throws IOException {
        requireReady();
        return parsed;
    }

    synchronized Installed installed() throws IOException {
        requireReady();
        if (installed == null) {
            throw new IOException("Fixture trust values cannot become installed startup trust");
        }
        return installed;
    }

    /** Read/hash facts only. Bootstrap JNI provenance and actual process/configuration checks remain separate. */
    synchronized SegmentClassicLaunchProgram verifyProgram(long deadlineNanos) throws IOException {
        requireReady();
        if (programAttempted) {
            throw new IOException("Controlled program verification is one-use");
        }
        programAttempted = true;
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadlineNanos);
            requireTime(deadlineNanos);
            byte[] bytes = read(root.resolve(recipeId + ".program"), 16, SegmentClassicLaunchProgram.MAX_BYTES);
            requireTime(deadlineNanos);
            var program = SegmentClassicLaunchProgram.decode(bytes);
            if (!parsed.recipe().program().equals(SegmentClassicLaunchProgram.identity(bytes))) {
                throw new IOException("Program bytes differ from the fixed recipe digest");
            }
            if (production && !program.files().get(program.aclJniFile()).path().equals(root.resolve("bootstrap")
                    .resolve(program.os() + "-" + program.architecture())
                    .resolve(program.os().equals("macos") ? "libnative_io.dylib" : "libnative_io.so"))) {
                throw new IOException("Program ACL JNI must use its fixed installed bootstrap path");
            }
            if (production) {
                var loaded = NativeIOBootstrap.controlledLibrary();
                var declared = program.files().get(program.aclJniFile());
                if (!loaded.path().equals(declared.path()) || loaded.length() != declared.length()
                        || !MessageDigest.isEqual(loaded.sha256(), declared.sha256().value())) {
                    throw new IOException("Program differs from the exact native-io library already loaded");
                }
            }
            for (var file : program.files()) {
                hashProgramFile(file, deadlineNanos);
            }
            verifyJdkTree(program, deadlineNanos);
            for (int jar : program.classPath()) {
                verifyJar(program.files().get(jar).path(), deadlineNanos);
            }
            requireTime(deadlineNanos);
            files.requireHealthy();
            if (!auxiliaryReads.isEmpty()) {
                throw new IOException("Controlled program auxiliary reads have not actually closed");
            }
            if (preload != null) {
                preload.verifyOriginal(deadlineNanos);
            }
            return program;
        } catch (IOException | RuntimeException | Error failure) {
            programFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Same fixed-root read owner and effective snapshot digest; still data, never a START context. */
    synchronized SegmentClassicLaunchConfiguration.Frozen verifyConfiguration(long deadlineNanos) throws IOException {
        requireReady();
        if (configurationAttempted) {
            throw new IOException("Controlled configuration verification is one-use");
        }
        configurationAttempted = true;
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadlineNanos);
            requireTime(deadlineNanos);
            byte[] bytes = read(root.resolve(recipeId + ".conf"), 1,
                    SegmentClassicLaunchConfiguration.MAX_BYTES, true);
            requireTime(deadlineNanos);
            var configuration = SegmentClassicLaunchConfiguration.parse(bytes);
            requireTime(deadlineNanos);
            if (!configuration.identity().equals(parsed.recipe().configuration())) {
                throw new IOException("Effective configuration differs from the fixed recipe digest");
            }
            files.requireHealthy();
            if (preload != null) {
                preload.verifyOriginal(deadlineNanos);
            }
            return configuration;
        } catch (IOException | RuntimeException | Error failure) {
            configurationFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Fixed parent-only leaf; these bytes are not installed issuer provenance or a Process observation. */
    synchronized byte[] readLauncherDescriptor() throws IOException {
        requireReady();
        if (launcherDescriptorAttempted) {
            throw new IOException("Original launcher descriptor read is one-use");
        }
        launcherDescriptorAttempted = true;
        return readLauncherPrivate(recipeId + ".launcher", 16, SegmentClassicLauncherInputs.MAX_V2_BYTES);
    }

    /** Key name is anchored by the original recipe, never by a command or a received packet. */
    synchronized byte[] readLauncherPrivateKey() throws IOException {
        requireReady();
        if (!launcherDescriptorAttempted || launcherKeyAttempted) {
            throw new IOException("Original launcher key follows its descriptor and is one-use");
        }
        launcherKeyAttempted = true;
        return readLauncherPrivate(parsed.recipe().keyName() + ".pk8", 48, 48);
    }

    synchronized void verifyLauncherDropper(SegmentClassicLauncherInputs input, long deadlineNanos)
            throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadlineNanos);
            input.requireRecipe(parsed);
            var file = input.dropper();
            Identity original = checked(file.path());
            if ((original.mode() & 0111) == 0) {
                throw new IOException("Approved exec-dropper is not executable");
            }
            hashProgramFile(file, deadlineNanos);
            byte[] header = new byte[32];
            try (FileChannel channel = SegmentOwnedFiles.open(files, file.path(), StandardOpenOption.READ,
                    LinkOption.NOFOLLOW_LINKS)) {
                SegmentFileIo.readFully(channel, ByteBuffer.wrap(header), 0);
            }
            int magic = ByteBuffer.wrap(header).getInt();
            ByteBuffer nativeHeader = ByteBuffer.wrap(header).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            boolean nativeExecutable;
            if (input.os().equals("linux")) {
                int kind = Short.toUnsignedInt(nativeHeader.getShort(16));
                int machine = input.architecture().equals("x86_64") ? 62 : 183;
                nativeExecutable = magic == 0x7f454c46 && header[4] == 2 && header[5] == 1 && header[6] == 1
                        && (kind == 2 || kind == 3) && Short.toUnsignedInt(nativeHeader.getShort(18)) == machine
                        && nativeHeader.getLong(24) != 0;
            } else {
                int machine = input.architecture().equals("x86_64") ? 0x01000007 : 0x0100000c;
                nativeExecutable = magic == 0xcffaedfe && nativeHeader.getInt(4) == machine
                        && nativeHeader.getInt(12) == 2;
            }
            if (!nativeExecutable) {
                throw new IOException("Approved exec-dropper requires a thin native executable of its exact platform");
            }
            if (!original.equals(checked(file.path()))) {
                throw new IOException("Approved exec-dropper changed during verification");
            }
            files.requireHealthy();
        } catch (IOException | RuntimeException | Error problem) {
            launcherFailure = problem;
            throw problem;
        } finally {
            initializingThread = null;
        }
    }

    private byte[] readLauncherPrivate(String leaf, int minimum, int maximum) throws IOException {
        initializingThread = Thread.currentThread();
        try {
            Path directory = root.resolve("launcher");
            Identity parent = checked(directory);
            if (!parent.directory() || parent.uid() != ownerUid || (parent.mode() & 07777) != 0700) {
                throw new IOException("Launcher material directory must be private to its fixed owner");
            }
            Path path = directory.resolve(leaf);
            Identity original = checked(path);
            if (original.uid() != ownerUid || (original.mode() & 07777) != 0600) {
                throw new IOException("Launcher descriptor and key require exact private file permissions");
            }
            byte[] bytes = read(path, minimum, maximum);
            if (!original.equals(checked(path)) || !parent.equals(checked(directory))) {
                throw new IOException("Original private launcher material changed during its read");
            }
            return bytes;
        } catch (IOException | RuntimeException | Error problem) {
            launcherFailure = problem;
            throw problem;
        } finally {
            initializingThread = null;
        }
    }

    private void requireReady() throws IOException {
        if (parsed == null || closing || initializationFailure != null || programFailure != null
                || configurationFailure != null || launcherFailure != null) {
            throw new IOException("Installed Classic trust is not ready or is closing",
                    initializationFailure != null ? initializationFailure
                            : programFailure != null ? programFailure
                            : configurationFailure != null ? configurationFailure : launcherFailure);
        }
        files.requireHealthy();
        if (preload != null) {
            preload.requireActive();
        }
    }

    private void requireBoundDeadline(long deadlineNanos) throws IOException {
        if (preload != null && preload.deadline() != deadlineNanos) {
            throw new IOException("Post-load verification must use the original preload deadline");
        }
    }

    synchronized Drain closeAndDrain() throws InterruptedException {
        if (initializingThread == Thread.currentThread()) {
            throw new IllegalStateException("Classic trust material reader cannot drain itself");
        }
        closing = true;
        for (var read : List.copyOf(auxiliaryReads)) {
            try {
                finishAuxiliary(read, null);
            } catch (IOException | RuntimeException | Error ignored) {
                // The original first result remains in the retained read and the returned diagnostic.
            }
        }
        var result = files.drain();
        return new Drain(result.drained() && auxiliaryReads.isEmpty(), initializationFailure, result,
                auxiliaryReads.size(), auxiliaryCloseFailure, programFailure, configurationFailure, launcherFailure);
    }

    /** Declared loading paths only; compares actual protected components without granting startup authority. */
    synchronized List<LinuxPathObservation> verifyLinuxLoadingPaths(SegmentClassicLinuxDeployment.Loading loading,
                                                                   long deadlineNanos) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadlineNanos);
            var first = observeLinuxLoadingPaths(loading, deadlineNanos);
            if (!first.equals(observeLinuxLoadingPaths(loading, deadlineNanos))) {
                throw new IOException("Original Linux loading paths changed between observations");
            }
            files.requireHealthy();
            requireTime(deadlineNanos);
            return first;
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private List<LinuxPathObservation> observeLinuxLoadingPaths(SegmentClassicLinuxDeployment.Loading loading,
                                                               long deadline) throws IOException {
        Map<Path, SegmentClassicLinuxDeployment.Alias> aliases = new HashMap<>();
        for (var alias : loading.aliases()) {
            aliases.put(alias.link(), alias);
        }
        var result = new ArrayList<LinuxPathObservation>();
        for (var alias : loading.aliases()) {
            result.add(observeLinuxPath(LinuxPathKind.ALIAS, alias.link(), aliases, true, deadline));
        }
        for (var directory : loading.directories()) {
            result.add(observeLinuxPath(LinuxPathKind.DIRECTORY, directory.path(), aliases, directory.present(),
                    deadline));
        }
        for (var configuration : loading.configuration()) {
            result.add(observeLinuxPath(LinuxPathKind.CONFIGURATION, configuration.path(), aliases,
                    configuration.file() >= 0, deadline));
        }
        result.add(observeLinuxPath(LinuxPathKind.INTERPRETER, loading.interpreter(), aliases, true, deadline));
        return List.copyOf(result);
    }

    /** Original iterator/open/first-close ownership; unrelated directory names never become approved files. */
    synchronized List<LinuxDirectoryObservation> verifyLinuxLoadingDirectories(
            SegmentClassicLinuxDeployment.Loading loading, long deadlineNanos) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadlineNanos);
            var first = observeLinuxLoadingDirectories(loading, deadlineNanos);
            if (!first.equals(observeLinuxLoadingDirectories(loading, deadlineNanos))) {
                throw new IOException("Original Linux loading directory members changed between observations");
            }
            files.requireHealthy();
            requireTime(deadlineNanos);
            return first;
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private List<LinuxDirectoryObservation> observeLinuxLoadingDirectories(
            SegmentClassicLinuxDeployment.Loading loading, long deadline) throws IOException {
        Map<Path, SegmentClassicLinuxDeployment.Alias> aliases = new HashMap<>();
        for (var alias : loading.aliases()) {
            aliases.put(alias.link(), alias);
        }
        var budget = new LoadingDirectoryBudget();
        Map<Path, List<String>> observed = new HashMap<>();
        var result = new ArrayList<LinuxDirectoryObservation>();
        for (var directory : loading.directories()) {
            var path = observeLinuxPath(LinuxPathKind.DIRECTORY, directory.path(), aliases,
                    directory.present(), deadline);
            List<String> names = List.of();
            if (path.identity() != null) {
                names = observed.get(path.resolved());
                if (names == null) {
                    names = enumerateLinuxDirectory(path.resolved(), budget, deadline);
                    observed.put(path.resolved(), names);
                }
            }
            if (!path.equals(observeLinuxPath(LinuxPathKind.DIRECTORY, directory.path(), aliases,
                    directory.present(), deadline))) {
                throw new IOException("Original loading directory or parent changed during enumeration");
            }
            result.add(new LinuxDirectoryObservation(path, names));
        }
        return List.copyOf(result);
    }

    private List<String> enumerateLinuxDirectory(Path directory, LoadingDirectoryBudget budget, long deadline)
            throws IOException {
        var names = new java.util.TreeSet<String>();
        var read = new AuxiliaryRead();
        auxiliaryReads.add(read);
        Throwable failure = null;
        try {
            requireTime(deadline);
            read.openStarted = true;
            DirectoryStream<Path> stream = Files.newDirectoryStream(directory);
            read.resource = stream;
            read.openReturned = true;
            for (Path member : stream) {
                requireTime(deadline);
                Path leaf = member.getFileName();
                String name = leaf == null ? "" : leaf.toString();
                if (!directory.equals(member.getParent()) || name.isEmpty() || name.equals(".") || name.equals("..")
                        || leaf.getNameCount() != 1 || !StandardCharsets.UTF_8.newEncoder().canEncode(name)
                        || !exactUtf8DirectoryName(leaf, name) || !names.add(name)) {
                    throw new IOException("Unsupported or duplicate loading directory member name");
                }
                budget.retain(name);
            }
        } catch (IOException | RuntimeException | Error problem) {
            failure = problem;
            throw problem;
        } finally {
            finishAuxiliary(read, failure);
        }
        requireTime(deadline);
        return List.copyOf(names);
    }

    private static boolean exactUtf8DirectoryName(Path leaf, String name) throws IOException {
        return leaf.equals(utf8LoadingPath("/" + name).getFileName());
    }

    private static Path utf8LoadingPath(String text) throws IOException {
        try {
            // The approved Unix provider decodes an ASCII file URI to native bytes, independent of JNU encoding.
            URI uri = new URI("file", "", text, null, null);
            String ascii = uri.toASCIIString();
            if (!ascii.startsWith("file:///") || uri.getRawAuthority() != null
                    || uri.getRawQuery() != null || uri.getRawFragment() != null) {
                throw new IOException("Loading member URI would use the JNU path fallback");
            }
            return Path.of(URI.create(ascii));
        } catch (URISyntaxException | IllegalArgumentException unsupported) {
            throw new IOException("Unsupported exact UTF-8 loading directory name", unsupported);
        }
    }

    private static void requireUtf8LoadingPath(Path path) throws IOException {
        String text = path.toString();
        if (!StandardCharsets.UTF_8.newEncoder().canEncode(text) || !path.equals(utf8LoadingPath(text))) {
            throw new IOException("Actual loading path bytes differ from strict UTF-8");
        }
    }

    private LinuxPathObservation observeLinuxPath(LinuxPathKind kind, Path entry,
            Map<Path, SegmentClassicLinuxDeployment.Alias> aliases, boolean present, long deadline) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (GeneralSecurityException unavailable) {
            throw new IOException("Required loading path SHA-256 is unavailable", unavailable);
        }
        requireTime(deadline);
        requireUtf8LoadingPath(entry);
        Path current = entry.getRoot();
        Identity parent = checked(current);
        observeLinuxComponent(digest, current, parent.parent());
        var pending = new ArrayDeque<String>();
        SegmentClassicLinuxDeployment.prependLoadingTarget(pending, entry.toString());
        int followed = 0;
        while (!pending.isEmpty()) {
            requireTime(deadline);
            String component = pending.removeFirst();
            if (component.equals(".") || component.equals("..")) {
                if (component.equals("..") && current.getParent() != null) {
                    current = current.getParent();
                }
                parent = checked(current);
                if (!parent.directory()) {
                    throw new IOException("Linux loading dot component requires a directory");
                }
                observeLinuxComponent(digest, current, pending.isEmpty() ? parent : parent.parent());
                if (pending.isEmpty()) {
                    return linuxLoadingLeaf(kind, entry, current, parent, present, digest, deadline);
                }
                continue;
            }
            Path candidate = current.resolve(component);
            String base = current.toString();
            if (!candidate.equals(utf8LoadingPath(base + (base.endsWith("/") ? "" : "/") + component))) {
                throw new IOException("Expanded Linux loading component differs from its original UTF-8 bytes");
            }
            requireUtf8LoadingPath(candidate);
            Identity raw;
            try {
                raw = SegmentClassicHandoffPaths.loadingIdentity(candidate);
            } catch (NoSuchFileException missing) {
                if (present || (parent.mode() & 022) != 0) {
                    throw new IOException("Linux path missing or absence parent permits creation", missing);
                }
                requireTime(deadline);
                Path unresolved = pending.isEmpty() ? candidate
                        : utf8LoadingPath(candidate + "/" + String.join("/", pending));
                requireUtf8LoadingPath(unresolved);
                return new LinuxPathObservation(kind, entry, unresolved, candidate, null,
                        new SegmentClassicHandoff.Digest(digest.digest()));
            }
            if ((parent.mode() & 022) != 0 && ((parent.mode() & 01000) == 0
                    || raw.uid() != parent.uid() && raw.uid() != ownerUid)) {
                throw new IOException("Linux loading parent permits replacement by an untrusted principal");
            }
            boolean last = pending.isEmpty();
            if ((raw.mode() & 0170000) == 0120000) {
                var alias = aliases.get(candidate);
                if (alias == null || raw.uid() != ownerUid || raw.links() != 1 || !present && last) {
                    throw new IOException("Undeclared, untrusted or falsely absent Linux loading alias");
                }
                readLoadingAlias(candidate, raw, alias, deadline);
                observeLinuxComponent(digest, candidate, raw);
                byte[] target = alias.target().getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(4).putInt(target.length).array());
                digest.update(target);
                if (kind == LinuxPathKind.ALIAS && last) {
                    if (!candidate.equals(entry)) {
                        throw new IOException("Declared alias leaf must use its original physical parent");
                    }
                    return new LinuxPathObservation(kind, entry, candidate, null, raw,
                            new SegmentClassicHandoff.Digest(digest.digest()));
                }
                if (++followed > SegmentClassicLinuxDeployment.MAX_ALIAS_FOLLOWS) {
                    throw new IOException("Cyclic or excessive Linux loading alias expansion");
                }
                if (alias.target().startsWith("/")) {
                    current = entry.getRoot();
                    parent = checked(current);
                    observeLinuxComponent(digest, current, parent.parent());
                }
                SegmentClassicLinuxDeployment.prependLoadingTarget(pending, alias.target());
                try {
                    SegmentClassicLinuxDeployment.requireLoadingPathCapacity(current, pending);
                } catch (IllegalArgumentException excessive) {
                    throw new IOException("Expanded Linux loading path exceeds its fixed capacity", excessive);
                }
                continue;
            }
            Identity identity = checked(candidate);
            if (!identity.equals(raw) || !last && !identity.directory()) {
                throw new IOException("Linux loading component changed or is not a directory");
            }
            observeLinuxComponent(digest, candidate, last ? identity : identity.parent());
            if (last) {
                return linuxLoadingLeaf(kind, entry, candidate, identity, present, digest, deadline);
            }
            current = candidate;
            parent = identity;
        }
        throw new IOException("Linux loading entry must name an explicit non-root path");
    }

    private static LinuxPathObservation linuxLoadingLeaf(LinuxPathKind kind, Path entry, Path resolved,
            Identity identity, boolean present, MessageDigest digest, long deadline) throws IOException {
        if (resolved.getParent() == null || !present || kind == LinuxPathKind.ALIAS
                || (kind == LinuxPathKind.DIRECTORY) != identity.directory() || (identity.mode() & 022) != 0) {
            throw new IOException("Linux loading leaf differs from its presence, kind or protection rule");
        }
        requireTime(deadline);
        return new LinuxPathObservation(kind, entry, resolved, null, identity,
                new SegmentClassicHandoff.Digest(digest.digest()));
    }

    private static void observeLinuxComponent(MessageDigest digest, Path path, Identity identity) {
        // A same-process comparison, not a persistent/signable encoding. No unbounded parent list is retained.
        for (String value : List.of(path.toString(), identity.toString())) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
            digest.update(bytes);
        }
    }

    private void readLoadingAlias(Path path, Identity before, SegmentClassicLinuxDeployment.Alias alias,
                                  long deadline) throws IOException {
        requireTime(deadline);
        requireUtf8LoadingPath(path);
        var link = nativeIo.observeSymbolicLink(path.toString());
        requireTime(deadline);
        Identity observed;
        try {
            observed = new Identity(link.device(), link.inode(), link.uid(), link.gid(), link.mode(), link.links(),
                    link.size(), FileTime.from(Instant.ofEpochSecond(link.modifiedSeconds(), link.modifiedNanos())),
                    FileTime.from(Instant.ofEpochSecond(link.changedSeconds(), link.changedNanos())), false);
        } catch (java.time.DateTimeException unsupported) {
            throw new IOException("Unsupported native loading alias timestamp", unsupported);
        }
        byte[] target = link.target();
        String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(target)).toString();
        if (!before.equals(observed) || !before.equals(SegmentClassicHandoffPaths.loadingIdentity(path))
                || !text.equals(alias.target())
                || !MessageDigest.isEqual(target, alias.target().getBytes(StandardCharsets.UTF_8))) {
            throw new IOException("Original loading alias identity or exact target bytes differ from declaration");
        }
        requireTime(deadline);
    }

    /** Same fixed file owner as v1; no loading alias is accepted by the ordinary file reader. */
    synchronized LinuxFileObservation verifyLinuxFile(SegmentClassicLinuxDeployment.File file, long deadlineNanos)
            throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadlineNanos);
            requireUtf8LoadingPath(file.path());
            if (file.role() == SegmentClassicLinuxDeployment.Role.LOADER_CACHE
                    && (file.length() < 48 || file.length() > SegmentClassicLinuxCache.MAX_BYTES)) {
                throw new IOException("Declared loader cache length exceeds its supported format bounds");
            }
            if (file.role() == SegmentClassicLinuxDeployment.Role.LOADER_CONFIG
                    && file.length() > SegmentClassicLinuxConfiguration.MAX_BYTES) {
                throw new IOException("Declared loader configuration length exceeds its supported bounds");
            }
            boolean executable = file.role().elf() && file.role() != SegmentClassicLinuxDeployment.Role.LIBRARY
                    && file.role() != SegmentClassicLinuxDeployment.Role.MODULE;
            var observed = hashFile(file.path(), file.length(), file.sha256(), executable, file.role().elf(),
                    file.role() == SegmentClassicLinuxDeployment.Role.LOADER_CACHE,
                    file.role() == SegmentClassicLinuxDeployment.Role.LOADER_CONFIG, deadlineNanos);
            if (executable && observed.elf().entry() == 0 || !executable && file.role().elf()
                    && observed.elf().type() != 3) {
                throw new IOException("Linux ELF type or entry differs from its declared file role");
            }
            return observed;
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private void hashProgramFile(SegmentClassicLaunchProgram.File file, long deadlineNanos) throws IOException {
        hashFile(file.path(), file.length(), file.sha256(), (file.roles() & SegmentClassicLaunchProgram.JAVA) != 0,
                false, false, false, deadlineNanos);
    }

    private LinuxFileObservation hashFile(Path path, long declaredLength, SegmentClassicHandoff.Digest sha256,
            boolean executable, boolean elf, boolean cache, boolean configuration, long deadlineNanos)
            throws IOException {
        requireTime(deadlineNanos);
        Identity before = checked(path);
        if (before.directory() || before.uid() != ownerUid || before.links() != 1
                || (before.mode() & 022) != 0 || before.size() != declaredLength
                || executable && (before.mode() & 0111) == 0) {
            throw new IOException("Program file identity, permissions or declared length mismatch");
        }
        var parents = chain(path);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (GeneralSecurityException unavailable) {
            throw new IOException("Required program SHA-256 is unavailable", unavailable);
        }
        SegmentClassicLinuxElf.Image image;
        SegmentClassicLinuxCache.Image cacheImage;
        SegmentClassicLinuxConfiguration.Image configurationImage;
        try (FileChannel channel = SegmentOwnedFiles.open(files, path,
                StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            requireTime(deadlineNanos);
            if (channel.size() != declaredLength) {
                throw new IOException("Program file length changed during open");
            }
            ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
            long remaining = declaredLength;
            while (remaining != 0) {
                requireTime(deadlineNanos);
                buffer.clear().limit((int) Math.min(buffer.capacity(), remaining));
                int length = channel.read(buffer);
                if (length <= 0) {
                    throw new IOException("Program file read made no progress before the declared end");
                }
                remaining -= length;
                buffer.flip();
                digest.update(buffer);
            }
            image = elf ? SegmentClassicLinuxElf.read(channel, declaredLength, deadlineNanos) : null;
            cacheImage = cache ? SegmentClassicLinuxCache.read(channel, declaredLength, deadlineNanos) : null;
            configurationImage = configuration
                    ? SegmentClassicLinuxConfiguration.read(channel, declaredLength, deadlineNanos) : null;
            if (channel.size() != declaredLength || !before.equals(checked(path))) {
                throw new IOException("Program file changed during its stream hash");
            }
        }
        requireTime(deadlineNanos);
        if (!before.equals(checked(path)) || !parents.equals(chain(path))
                || !MessageDigest.isEqual(sha256.value(), digest.digest())) {
            throw new IOException("Program file, parent or SHA-256 differs from its fixed declaration");
        }
        files.requireHealthy();
        requireTime(deadlineNanos);
        return new LinuxFileObservation(before, parents, image, cacheImage, configurationImage);
    }

    private void verifyJdkTree(SegmentClassicLaunchProgram program, long deadlineNanos) throws IOException {
        Set<Path> declared = new HashSet<>();
        for (var file : program.files()) {
            if ((file.roles() & SegmentClassicLaunchProgram.JDK) != 0) {
                declared.add(file.path());
            }
        }
        Set<Path> seen = new HashSet<>();
        var pending = new ArrayDeque<Path>();
        pending.add(program.javaHome());
        int directories = 0;
        while (!pending.isEmpty()) {
            requireTime(deadlineNanos);
            if (++directories > SegmentClassicLaunchProgram.MAX_FILES) {
                throw new IOException("Controlled JDK directory count exceeds its fixed capacity");
            }
            Path path = pending.removeFirst();
            Identity before = checked(path);
            if (!before.directory() || (before.mode() & 022) != 0) {
                throw new IOException("Controlled JDK member is not a protected directory");
            }
            var parents = chain(path);
            var read = new AuxiliaryRead();
            auxiliaryReads.add(read);
            Throwable failure = null;
            try {
                read.openStarted = true;
                DirectoryStream<Path> stream = Files.newDirectoryStream(path);
                read.resource = stream;
                read.openReturned = true;
                for (Path member : stream) {
                    requireTime(deadlineNanos);
                    Identity identity = checked(member);
                    if (identity.directory()) {
                        if (pending.size() >= SegmentClassicLaunchProgram.MAX_FILES) {
                            throw new IOException("Controlled JDK pending directory capacity exhausted");
                        }
                        pending.addLast(member);
                    } else if (!declared.contains(member) || !seen.add(member)) {
                        throw new IOException("JDK contains an undeclared or repeated program file");
                    }
                }
            } catch (IOException | RuntimeException | Error problem) {
                failure = problem;
                throw problem;
            } finally {
                finishAuxiliary(read, failure);
            }
            if (!before.equals(checked(path)) || !parents.equals(chain(path))) {
                throw new IOException("JDK directory changed during enumeration");
            }
        }
        if (!declared.equals(seen)) {
            throw new IOException("JDK program manifest omits a required member");
        }
    }

    private void verifyJar(Path path, long deadlineNanos) throws IOException {
        requireTime(deadlineNanos);
        Identity before = checked(path);
        var parents = chain(path);
        var read = new AuxiliaryRead();
        auxiliaryReads.add(read);
        Throwable failure = null;
        try {
            read.openStarted = true;
            JarFile jar = new JarFile(path.toFile(), false);
            read.resource = jar;
            read.openReturned = true;
            java.util.jar.JarEntry manifestEntry = null;
            var entries = jar.entries();
            while (entries.hasMoreElements()) {
                requireTime(deadlineNanos);
                var entry = entries.nextElement();
                if (entry.getName().equalsIgnoreCase("META-INF/INDEX.LIST")) {
                    throw new IOException("Implicit JAR index dependencies are unsupported");
                }
                if (entry.getName().equalsIgnoreCase("META-INF/MANIFEST.MF")) {
                    if (manifestEntry != null || entry.isDirectory() || entry.getSize() > 65536) {
                        throw new IOException("Ambiguous or oversized JAR manifest");
                    }
                    manifestEntry = entry;
                }
            }
            if (manifestEntry != null) {
                var streamRead = new AuxiliaryRead();
                auxiliaryReads.add(streamRead);
                Throwable streamFailure = null;
                try {
                    streamRead.openStarted = true;
                    InputStream stream = jar.getInputStream(manifestEntry);
                    streamRead.resource = stream;
                    streamRead.openReturned = true;
                    if (stream == null) {
                        throw new IOException("Program JAR manifest input is missing");
                    }
                    byte[] bytes = stream.readNBytes(65537);
                    requireTime(deadlineNanos);
                    if (bytes.length > 65536) {
                        throw new IOException("JAR manifest exceeds its bounded read");
                    }
                    var manifest = new Manifest(new ByteArrayInputStream(bytes));
                    if (manifest.getMainAttributes().containsKey(Attributes.Name.CLASS_PATH)) {
                        throw new IOException("Implicit JAR manifest Class-Path is unsupported");
                    }
                } catch (IOException | RuntimeException | Error problem) {
                    streamFailure = problem;
                    throw problem;
                } finally {
                    finishAuxiliary(streamRead, streamFailure);
                }
            }
        } catch (IOException | RuntimeException | Error problem) {
            failure = problem;
            throw problem;
        } finally {
            finishAuxiliary(read, failure);
        }
        if (!before.equals(checked(path)) || !parents.equals(chain(path))) {
            throw new IOException("Program JAR changed during manifest verification");
        }
    }

    private static void requireTime(long deadlineNanos) throws IOException {
        if (deadlineNanos - System.nanoTime() <= 0) {
            throw new IOException("Controlled program verification deadline elapsed");
        }
    }

    private void finishAuxiliary(AuxiliaryRead read, Throwable operationFailure) throws IOException {
        if (read.closeAttempted || read.failure != null) {
            return;
        }
        if (read.resource == null) {
            if (read.openStarted && !read.openReturned) {
                // ZipFile construction can swallow a native close error while reporting a parse error.
                // No returned handle means this owner cannot observe or retry that original native result.
                read.failure = new IOException("Program auxiliary acquisition has no observed native close result",
                        operationFailure);
                if (auxiliaryCloseFailure == null) {
                    auxiliaryCloseFailure = read.failure;
                }
                return;
            }
            auxiliaryReads.remove(read);
            return;
        }
        read.closeAttempted = true;
        try {
            read.resource.close();
            read.resource = null;
            auxiliaryReads.remove(read);
        } catch (Throwable failure) {
            read.failure = failure;
            if (auxiliaryCloseFailure == null) {
                auxiliaryCloseFailure = failure;
            }
            if (operationFailure != null) {
                if (operationFailure != failure) {
                    operationFailure.addSuppressed(failure);
                }
            } else if (failure instanceof IOException io) {
                throw io;
            } else if (failure instanceof RuntimeException runtime) {
                throw runtime;
            } else if (failure instanceof Error error) {
                throw error;
            } else {
                throw new IOException("Controlled program auxiliary native close failed", failure);
            }
        }
    }

    private Identity requireRoot() throws IOException {
        Identity identity = checked(root);
        if (!identity.directory() || (identity.mode() & 022) != 0) {
            throw new IOException("Installed trust root must be an existing protected directory");
        }
        return identity;
    }

    private Identity checked(Path path) throws IOException {
        Identity identity = SegmentClassicHandoffPaths.fileIdentity(path);
        if (identity.uid() != ownerUid && !(identity.directory() && identity.uid() == 0)) {
            throw new IOException("Untrusted installed material path owner");
        }
        if (nativeIo.hasExtendedAcl(path.toString())) {
            throw new IOException("Additional ACL on a fixed Classic trust path is unsupported");
        }
        if (!identity.equals(SegmentClassicHandoffPaths.fileIdentity(path))) {
            throw new IOException("Installed trust path changed during its permission query");
        }
        return identity;
    }

    private List<Identity> chain(Path path) throws IOException {
        List<Identity> parents = new ArrayList<>();
        Identity child = checked(path);
        for (Path parent = path.getParent(); parent != null; parent = parent.getParent()) {
            Identity identity = checked(parent);
            if (!identity.directory() || (identity.mode() & 022) != 0
                    && ((identity.mode() & 01000) == 0 || child.uid() != identity.uid() && child.uid() != ownerUid)) {
                throw new IOException("Installed trust parent permits replacement by an untrusted principal");
            }
            parents.add(identity.parent());
            child = identity;
        }
        return List.copyOf(parents);
    }

    private byte[] read(Path path, int minimum, int maximum) throws IOException {
        return read(path, minimum, maximum, false);
    }

    private byte[] read(Path path, int minimum, int maximum, boolean configuration) throws IOException {
        Identity before = checked(path);
        if (before.directory() || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                || before.uid() != ownerUid || before.links() != 1 || (before.mode() & 022) != 0
                || before.size() < minimum || before.size() > maximum) {
            throw new IOException("Installed material must be an existing protected single-link bounded file");
        }
        if (configuration && ((before.mode() & 007) != 0 || production && ((before.mode() & 0040) == 0
                || before.gid() != parsed.recipe().primaryGid() && !parsed.recipe().groups().contains(before.gid())))) {
            throw new IOException("Credential-bearing configuration requires only the expected old-UID read group");
        }
        var parents = chain(path);
        byte[] bytes;
        try (FileChannel channel = SegmentOwnedFiles.open(files, path, StandardOpenOption.READ,
                LinkOption.NOFOLLOW_LINKS)) {
            if (channel.size() != before.size()) {
                throw new IOException("Installed material size changed during open");
            }
            bytes = new byte[(int) before.size()];
            SegmentFileIo.readFully(channel, ByteBuffer.wrap(bytes), 0);
            if (channel.size() != before.size()) {
                throw new IOException("Installed material size changed during read");
            }
        }
        if (!before.equals(checked(path)) || !parents.equals(chain(path))) {
            throw new IOException("Installed material or parent identity changed during the read");
        }
        files.requireHealthy();
        if (preload != null) {
            preload.match(path, before, bytes);
        }
        return bytes;
    }

    /** Pure bounded recipe data parsing, also used before JNI is initialized. */
    static Recipe decode(byte[] bytes) throws IOException {
        try {
            ByteBuffer b = ByteBuffer.wrap(bytes);
            if (b.getLong() != MAGIC || b.getShort() != 1 || b.getShort() != 0
                    || b.getInt() != bytes.length - 16) {
                throw new IllegalArgumentException("Unknown installed Classic recipe framing");
            }
            String id = text(b, 128);
            String deployment = text(b, 1024);
            String keyName = text(b, 128);
            var signer = digest(b);
            var program = digest(b);
            var config = digest(b);
            var scope = digest(b);
            long uid = Integer.toUnsignedLong(b.getInt());
            long gid = Integer.toUnsignedLong(b.getInt());
            int count = Short.toUnsignedInt(b.getShort());
            if (count > 64 || count * 4 != b.remaining()) {
                throw new IllegalArgumentException("Invalid installed credential group count");
            }
            List<Long> groups = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                groups.add(Integer.toUnsignedLong(b.getInt()));
            }
            return new Recipe(id, deployment, keyName, signer, program, config, scope, uid, gid, groups);
        } catch (IllegalArgumentException | BufferUnderflowException malformed) {
            throw new IOException("Invalid installed Classic recipe", malformed);
        }
    }

    private static SegmentClassicHandoff.Digest digest(ByteBuffer b) {
        byte[] bytes = new byte[32];
        b.get(bytes);
        return new SegmentClassicHandoff.Digest(bytes);
    }

    private static String text(ByteBuffer b, int maximum) throws IOException {
        int length = Short.toUnsignedInt(b.getShort());
        if (length < 1 || length > maximum || length > b.remaining()) {
            throw new IllegalArgumentException("Invalid installed recipe text length");
        }
        byte[] bytes = new byte[length];
        b.get(bytes);
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
    }
}

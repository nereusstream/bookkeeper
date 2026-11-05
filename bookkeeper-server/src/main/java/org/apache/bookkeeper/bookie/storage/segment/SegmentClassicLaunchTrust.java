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
import java.net.StandardProtocolFamily;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnixDomainSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicHandoffPaths.Identity;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLinuxDeploymentV2.ConfigurationKind;
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
    private static final List<String> BUILTIN_HWCAP_SUFFIXES = List.of("glibc-hwcaps/x86-64-v4/",
            "glibc-hwcaps/x86-64-v3/", "glibc-hwcaps/x86-64-v2/", "");
    private static final long DF_1_NODEFLIB = 0x800L;
    private static final String CGROUP_ROOT = "/sys/fs/cgroup";
    private static final long SYSFS_TYPE = 0x62656572L;
    private static final String SYSTEMD_PRIVATE_SOCKET = "/run/systemd/private";

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

    /** One original unit reference; dropIn is -1 for its fragment, otherwise the original drop-in position. */
    record LinuxUnitFileObservation(int unit, int dropIn, int member, Path path,
                                    SegmentClassicHandoff.Digest sha256, LinuxFileObservation material) { }

    /** Actual disk link and original relationship only; not systemd lookup priority or an active dependency. */
    record LinuxInstallLinkObservation(int link, SegmentClassicLinuxDeploymentV2.InstallLink declaration,
                                       LinuxPathObservation path) { }

    /** Original member observations only; not a loader choice, installed provenance or qualification. */
    record LinuxElfMemberObservation(int member, Identity identity, List<Identity> parents,
                                     SegmentClassicHandoff.Digest sha256, SegmentClassicLinuxElf.Image2 elf) {
        LinuxElfMemberObservation {
            parents = List.copyOf(parents);
        }
    }

    /** Context -1 marks only the fixed interpreter result; it is never a context-selection fallback. */
    record LinuxLoadPathObservation(int context, int member, String logical, LinuxPathObservation path,
                                     LinuxElfMemberObservation content) { }

    record LinuxNeededContextObservation(SegmentClassicLinuxDeploymentV2.NeededRule declaration, String name) { }

    /** Conditional literal absolute slash branch only; no preceding name miss, mapping or winner is proved. */
    record LinuxNeededDirectObservation(int rule, SegmentClassicLinuxDeploymentV2.NeededRule declaration, String name,
            LinuxLoadPathObservation requester, LinuxPathObservation path, LinuxLoadPathObservation candidate) { }

    /** One base-directory lookup only. A null candidate records reliable absence, never a search winner. */
    record LinuxNeededLookupObservation(int rule, int directory,
            SegmentClassicLinuxDeploymentV2.NeededRule declaration, String logical,
            LinuxPathObservation directoryPath, LinuxPathObservation path, LinuxLoadPathObservation requester,
            LinuxLoadPathObservation candidate) { }

    /** Fixed built-in position only, not an active CPU capability or a loader attempt/winner. */
    record LinuxNeededDirectoryPosition(int position, String suffix, String logical, LinuxPathObservation path,
                                        LinuxLoadPathObservation candidate) { }

    /** All four unfiltered built-in positions for one original directory; no custom prepend/mask is observed. */
    record LinuxNeededDirectoryObservation(int rule, int directory,
            SegmentClassicLinuxDeploymentV2.NeededRule declaration, String name, LinuxPathObservation directoryPath,
            LinuxLoadPathObservation requester, List<LinuxNeededDirectoryPosition> positions) {
        LinuxNeededDirectoryObservation {
            positions = List.copyOf(positions);
        }
    }

    enum LinuxDefaultState { BUILTIN_POSITIONS_OBSERVED, SUPPRESSED_NODEFLIB }

    record LinuxDefaultDirectoryObservation(int policyOrdinal, LinuxNeededDirectoryObservation observation) { }

    /** Conditional default-stage facts, never proof that preceding search stages failed or a winner exists. */
    record LinuxNeededDefaultObservation(int rule, SegmentClassicLinuxDeploymentV2.NeededRule declaration,
            String name, LinuxLoadPathObservation requester, boolean flags1Present, long flags1,
            LinuxDefaultState state, List<LinuxDefaultDirectoryObservation> directories) {
        LinuxNeededDefaultObservation {
            directories = List.copyOf(directories);
        }
    }

    private record LinuxDirectoryPlan(int directory, List<String> logicals) { }

    private record LinuxDirectoryPaths(int directory, LinuxPathObservation directoryPath,
                                       List<LinuxPathObservation> positions) { }

    private record LinuxNeededDefaultPlan(SegmentClassicLinuxDeploymentV2.NeededRule rule,
            SegmentClassicLinuxDeploymentV2.Context context, SegmentClassicLinuxDeploymentV2.Elf metadata,
            String name, List<LinuxDirectoryPlan> directories) { }

    enum LinuxRequesterTag { NONE, RPATH, RUNPATH }

    private record LinuxNeededRequesterTagPlan(SegmentClassicLinuxDeploymentV2.NeededRule rule,
            SegmentClassicLinuxDeploymentV2.Context context, SegmentClassicLinuxDeploymentV2.Elf metadata,
            String name, LinuxRequesterTag tag, SegmentClassicLinuxDeploymentV2.DynamicPath declaration,
            List<LinuxDirectoryPlan> directories) { }

    /** This requester's own tag facts only; stage reachability, inhibition and active positions remain unobserved. */
    record LinuxNeededRequesterTagObservation(int rule, SegmentClassicLinuxDeploymentV2.NeededRule declaration,
            String name, LinuxRequesterTag tag, SegmentClassicLinuxDeploymentV2.DynamicPath tagDeclaration,
            LinuxLoadPathObservation requester, List<LinuxNeededDirectoryObservation> directories) {
        LinuxNeededRequesterTagObservation {
            directories = List.copyOf(directories);
        }
    }

    /** One numeric-equivalent LP64 cache row, not capability-filtered and never a selected winner. */
    record LinuxNeededCacheRow(SegmentClassicLinuxCache.Entry entry, String logical, LinuxPathObservation path,
                               LinuxLoadPathObservation candidate) { }

    /** Cache absence, an empty row group and stale row targets retain distinct observations. */
    record LinuxNeededCacheObservation(int rule, SegmentClassicLinuxDeploymentV2.NeededRule declaration,
            String name, LinuxLoadPathObservation requester, LinuxLoaderConfigurationObservation cache,
            List<LinuxNeededCacheRow> rows) {
        LinuxNeededCacheObservation {
            rows = List.copyOf(rows);
        }
    }

    /** Original declaration indices only, never a selected Process backend or permission to execute. */
    record LinuxJdkHelperReference(int program, int member, int context, String logical) { }

    /** Helper material and its actual PT_INTERP association only; NEEDED candidates are not a proved closure. */
    record LinuxJdkHelperObservation(LinuxJdkHelperReference declaration, LinuxLoadPathObservation helper,
                                     LinuxLoadPathObservation interpreter) { }

    /** This owner's cached backend/path and original helper material, never permission to execute either. */
    static final class LinuxJdkProcessObservation {
        private final NativeIO.JdkProcessLaunchObservation cached;
        private final LinuxJdkHelperObservation helper;

        private LinuxJdkProcessObservation(NativeIO.JdkProcessLaunchObservation cached,
                LinuxJdkHelperObservation helper) {
            this.cached = cached;
            this.helper = helper;
        }

        NativeIO.JdkProcessLaunchObservation cached() {
            return cached;
        }

        LinuxJdkHelperObservation helper() {
            return helper;
        }
    }

    /** Only this original owner can bind raw diagnostic output to its actual loader observation. */
    static final class LinuxLoaderDiagnosticsObservation {
        private final LinuxLoadPathObservation loader;
        private final byte[] stdout;

        private LinuxLoaderDiagnosticsObservation(LinuxLoadPathObservation loader, byte[] stdout) {
            this.loader = loader;
            this.stdout = stdout.clone();
        }

        LinuxLoadPathObservation loader() {
            return loader;
        }

        byte[] stdout() {
            return stdout.clone();
        }
    }

    /** Entire declared context set only; no cross-object snapshot, search closure or loader winner is implied. */
    record LinuxCandidateContextObservation(List<LinuxLoadPathObservation> contexts,
            LinuxLoadPathObservation interpreter, List<LinuxNeededContextObservation> needed,
            List<SegmentClassicLinuxDeploymentV2.ModuleRequest> modules) {
        LinuxCandidateContextObservation {
            contexts = List.copyOf(contexts);
            needed = List.copyOf(needed);
            modules = List.copyOf(modules);
        }
    }

    enum LinuxContentKind { NONE, ELF1, ELF2, CACHE, LDCONFIG }

    /** Data read from a caller-held channel; no source authority or native close result is implied. */
    record LinuxContent(SegmentClassicHandoff.Digest sha256, SegmentClassicLinuxElf.Image elf,
                        SegmentClassicLinuxElf.Image2 elf2, SegmentClassicLinuxCache.Image cache,
                        SegmentClassicLinuxConfiguration.Image configuration) { }

    private record HashedFile(Identity identity, List<Identity> parents, LinuxContent content) {
        private HashedFile {
            parents = List.copyOf(parents);
        }

        LinuxFileObservation legacy() {
            return new LinuxFileObservation(identity, parents, content.elf(), content.cache(), content.configuration());
        }
    }

    enum LinuxPathKind { ALIAS, DIRECTORY, CONFIGURATION, INTERPRETER, LOOKUP, LITERAL }

    /** Bounded path facts only. The chain digest compares two observations, never startup authority. */
    record LinuxPathObservation(LinuxPathKind kind, Path entry, Path resolved, Path absentAt, Identity identity,
                                SegmentClassicHandoff.Digest chain) { }

    private static final class LookupNotDirectory extends IOException {
        private static final long serialVersionUID = 1L;
        private final LinuxPathObservation observed;

        private LookupNotDirectory(LinuxPathObservation observed) {
            super("Observed ordinary file cannot be traversed as a directory");
            this.observed = observed;
        }
    }

    private static boolean lookupKind(LinuxPathKind kind) {
        return kind == LinuxPathKind.LOOKUP || kind == LinuxPathKind.LITERAL;
    }

    /** Complete observed names only, not approved library candidates or a loader selection. */
    record LinuxDirectoryObservation(LinuxPathObservation path, List<String> members) {
        LinuxDirectoryObservation {
            members = List.copyOf(members);
        }

        /** Ordinary physical members only; the filesystem glob layer must also handle literal paths and dot entries. */
        List<String> matchingMembers(String component, long deadline) throws IOException {
            return SegmentClassicLinuxGlob.matchingMembers(members, component, deadline);
        }
    }

    /** Original directory index and logical spelling only, never a resolved-path or startup permit. */
    record LinuxSearchDirectory(int directory, String logical, boolean present) {
        LinuxSearchDirectory {
            Objects.requireNonNull(logical, "logical");
            if (directory < 0 || directory >= 64) {
                throw new IllegalArgumentException("Directory index is outside the original bounded table");
            }
        }
    }

    record LinuxSearchDirectoryObservation(int directory, String logical, boolean present, LinuxPathObservation path,
                                            List<String> members) {
        LinuxSearchDirectoryObservation {
            members = List.copyOf(members);
        }

        LinuxDirectoryObservation legacy() {
            return new LinuxDirectoryObservation(path, members);
        }
    }

    record LinuxConfigurationFile(String logical, int file, Identity identity,
                                  SegmentClassicLinuxConfiguration.Image content) { }

    record LinuxConfigurationSearch(String logical, LinuxPathObservation path, int members) { }

    record LinuxConfigurationInclude(int source, int line, int column, int word, String pattern,
            List<LinuxConfigurationSearch> directories, List<String> matches) {
        LinuxConfigurationInclude {
            directories = List.copyOf(directories);
            matches = List.copyOf(matches);
        }
    }

    /** Entire bounded observation pass only. No deployment approval, Installed, START or authority is issued. */
    record LinuxConfigurationGraph(boolean absent, SegmentClassicLinuxConfiguration.Usage usage,
            List<LinuxConfigurationFile> files, List<LinuxConfigurationInclude> includes,
            List<LinuxDirectoryObservation> directories, SegmentClassicHandoff.Digest observed) {
        LinuxConfigurationGraph {
            files = List.copyOf(files);
            includes = List.copyOf(includes);
            directories = List.copyOf(directories);
        }
    }

    private record ConfigurationContext(long device, long inode, String directory) { }

    private record LoadingLookup(LinuxPathObservation path, boolean complete) { }

    /** Original material facts for data scanning only; file indices are never renumbered. */
    record LinuxConfigurationMaterial(int file, Path path, long length, SegmentClassicHandoff.Digest sha256,
                                      ConfigurationKind kind) {
        LinuxConfigurationMaterial {
            Objects.requireNonNull(path, "path");
            Objects.requireNonNull(sha256, "sha256");
            Objects.requireNonNull(kind, "kind");
            if (file < 0 || file >= 1024 || length < 0 || kind == ConfigurationKind.PRELOAD_ABSENT) {
                throw new IllegalArgumentException("Invalid present configuration material");
            }
        }
    }

    /** One original configuration rule's data observation, never loader choice or qualification. */
    record LinuxLoaderConfigurationObservation(int rule, ConfigurationKind kind, LinuxPathObservation path,
            SegmentClassicHandoff.Digest sha256, LinuxFileObservation material, LinuxConfigurationGraph graph) { }

    static final class LoadingDirectoryBudget {
        private int entries;
        private int nameBytes;

        void retain(String name) throws IOException {
            retain(name.getBytes(StandardCharsets.UTF_8).length);
        }

        void retain(int bytes) throws IOException {
            if (bytes < 0 || entries >= MAX_LOADING_DIRECTORY_ENTRIES
                    || bytes > MAX_LOADING_DIRECTORY_NAME_BYTES - nameBytes) {
                throw new IOException("Loading directory observations exceed their fixed total name capacity");
            }
            entries++;
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
                 Throwable configurationFailure, Throwable launcherFailure, boolean diagnosticsInFlight,
                 boolean diagnosticsStartClaimed, boolean diagnosticsDrained, Throwable diagnosticsFailure,
                 boolean backendInFlight, Throwable backendFailure) { }

    private static final class AuxiliaryRead {
        AutoCloseable resource;
        boolean openStarted;
        boolean openReturned;
        boolean closeAttempted;
        Throwable failure;
    }

    private record HeldCgroupRoot(Path path, FileChannel channel, NativeIO.DirectoryFileSystem facts,
                                  List<Identity> identities) { }

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
    private SegmentClassicLaunchProgram.Decoded verifiedProgram;
    private LinuxFileObservation verifiedRuntimeImage;
    private HeldCgroupRoot linuxCgroupRoot;
    private UUID linuxBootId;
    private NativeIO.LinuxProcessBirth linuxProcessBirth;
    private boolean configurationAttempted;
    private Throwable configurationFailure;
    private boolean launcherDescriptorAttempted;
    private boolean launcherKeyAttempted;
    private Throwable launcherFailure;
    private SegmentClassicLauncherInputs.Decoded launcherSource;
    private SegmentClassicLauncherInputs launcherInput;
    private SegmentClassicHandoff.Digest launcherSourceIdentity;
    private LinuxProvider providerAtDecode;
    private LinuxProvider linuxProvider;
    private boolean linuxProviderAttempted;
    private Throwable auxiliaryCloseFailure;
    private boolean diagnosticsAttempted;
    private boolean diagnosticsInFlight;
    private boolean diagnosticsStartClaimed;
    private Thread diagnosticsCaller;
    private SegmentClassicSystemdQuery.Output diagnosticsOutput;
    private Throwable diagnosticsFailure;
    private boolean backendAttempted;
    private boolean backendInFlight;
    private Thread backendCaller;
    private Throwable backendFailure;
    private LinuxJdkProcessObservation backendObservation;
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
            var decoded = SegmentClassicLaunchProgram.decodeWithRawPaths(bytes);
            var program = decoded.value();
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
            LinuxFileObservation runtimeImage = null;
            for (var file : program.files()) {
                var observed = hashProgramFile(file, deadlineNanos);
                if (file.path().equals(program.javaHome().resolve("lib/modules"))) {
                    runtimeImage = observed;
                }
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
            verifiedProgram = decoded;
            verifiedRuntimeImage = runtimeImage;
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

    /** Same one-use descriptor source; raw text remains private data, never a transferable provider permit. */
    synchronized SegmentClassicLauncherInputs readLauncherInputs(long deadlineNanos) throws IOException {
        requireReady();
        try {
            requireBoundDeadline(deadlineNanos);
            requireTime(deadlineNanos);
            byte[] bytes = readLauncherDescriptor();
            initializingThread = Thread.currentThread();
            var decoded = SegmentClassicLauncherInputs.decodeWithRawPaths(bytes);
            LinuxProvider candidate = null;
            if (decoded.linuxDeployment() != null && decoded.linuxDeployment().schema() == 2) {
                // Inspect the already-used bootstrap default filesystem before constructing candidate Paths.
                candidate = linuxProviderObjects();
                if (verifiedProgram == null || verifiedRuntimeImage == null) {
                    throw new IOException("D2 decoding requires this owner's original verified program/image");
                }
            }
            var value = decoded.value();
            requireTime(deadlineNanos);
            files.requireHealthy();
            launcherSource = decoded;
            launcherInput = value;
            launcherSourceIdentity = SegmentClassicHandoff.digest(bytes);
            providerAtDecode = candidate;
            return value;
        } catch (IOException | RuntimeException | Error problem) {
            launcherFailure = problem;
            throw problem;
        } finally {
            initializingThread = null;
        }
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

    private record LinuxProvider(FileSystem filesystem, FileSystemProvider provider, Class<?> pathClass) { }

    /** Fixed implementation objects under the original bootstrap; no candidate Path is queried to discover them. */
    private static LinuxProvider linuxProviderObjects() throws IOException {
        FileSystem filesystem = FileSystems.getDefault();
        requireBootstrapType(filesystem.getClass(), "sun.nio.fs.LinuxFileSystem");
        FileSystemProvider provider = filesystem.provider();
        requireBootstrapType(provider.getClass(), "sun.nio.fs.LinuxFileSystemProvider");
        Path rootPath = filesystem.getPath("/");
        requireBootstrapType(rootPath.getClass(), "sun.nio.fs.UnixPath");
        requireBootstrapType(URI.class, "java.net.URI");
        try {
            requireBootstrapType(Class.forName("sun.nio.fs.UnixUriUtils", false, null), "sun.nio.fs.UnixUriUtils");
        } catch (ClassNotFoundException unavailable) {
            throw new IOException("Approved bootstrap Unix URI implementation is unavailable", unavailable);
        }
        var module = ModuleLayer.boot().configuration().findModule("java.base");
        if (module.isEmpty() || module.get().reference().location().isEmpty()
                || !module.get().reference().location().get().equals(URI.create("jrt:/java.base"))) {
            throw new IOException("Default filesystem implementation is outside the original runtime module image");
        }
        return new LinuxProvider(filesystem, provider, rootPath.getClass());
    }

    private static void requireBootstrapType(Class<?> actual, String name) throws IOException {
        try {
            if (!actual.getName().equals(name) || actual != Class.forName(name, false, null)
                    || actual.getClassLoader() != null || actual.getModule() != URI.class.getModule()
                    || actual.getModule().getLayer() != ModuleLayer.boot()) {
                throw new IOException("Unsupported default filesystem/provider/Path/URI implementation source");
            }
        } catch (ClassNotFoundException unavailable) {
            throw new IOException("Approved bootstrap implementation type is unavailable", unavailable);
        }
    }

    private static boolean sameProvider(LinuxProvider first, LinuxProvider second) {
        return first != null && second != null && first.filesystem() == second.filesystem()
                && first.provider() == second.provider() && first.pathClass() == second.pathClass();
    }

    /** Byte comparison only under an already trusted bootstrap. No provider approval, path lookup or permit. */
    static String requireUtf8LoadingText(String original, long deadline) throws IOException {
        requireTime(deadline);
        if (original == null || original.isEmpty() || original.length() > 4096 || original.indexOf(0) >= 0
                || !StandardCharsets.UTF_8.newEncoder().canEncode(original)
                || original.getBytes(StandardCharsets.UTF_8).length > 4096) {
            throw new IOException("Invalid bounded original Linux loading text");
        }
        for (String component : original.split("/", -1)) {
            requireTime(deadline);
            if (component.isEmpty() || component.equals(".") || component.equals("..")) {
                continue; // The logical walker must still retain separators, dot traversal and final directory checks.
            }
            Path actual = Path.of("/").resolve(component);
            if (!actual.equals(utf8LoadingPath("/" + component))) {
                throw new IOException("Original Linux name component differs from its strict UTF8 bytes");
            }
        }
        requireTime(deadline);
        return original;
    }

    private static void requirePhysicalBytes(String original, Path actual, LinuxProvider provider, long deadline)
            throws IOException {
        requireUtf8LoadingText(original, deadline);
        // getClass is checked before invoking any candidate Path methods.
        if (actual.getClass() != provider.pathClass()) {
            throw new IOException("Candidate path has an unapproved implementation");
        }
        if (actual.getFileSystem() != provider.filesystem()) {
            throw new IOException("Candidate path belongs to another filesystem");
        }
        Path expected = utf8LoadingPath(original);
        if (expected.getClass() != provider.pathClass() || expected.getFileSystem() != provider.filesystem()
                || !actual.equals(expected) || !actual.toString().equals(original)) {
            throw new IOException("Original physical path text, native bytes or JNI string differ");
        }
        requireTime(deadline);
    }

    /** Owner-private provider/source binding only. Bootstrap TCB is a premise, not inferred from a later image hash. */
    synchronized void verifyLinuxProvider(long deadlineNanos) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadlineNanos);
            requireTime(deadlineNanos);
            if (linuxProviderAttempted) {
                throw new IOException("Original Linux provider verification is one-use");
            }
            linuxProviderAttempted = true;
            if (!production || installed == null || preload == null || verifiedProgram == null
                    || verifiedRuntimeImage == null || launcherSource == null || launcherSourceIdentity == null
                    || launcherInput == null || launcherInput.linuxDeployment() == null
                    || launcherInput.linuxDeployment().schema() != 2) {
                throw new IOException("Linux provider binding requires the original installed program and descriptor");
            }
            LinuxProvider candidate = linuxProviderObjects();
            if (!sameProvider(candidate, providerAtDecode)) {
                throw new IOException("Default implementation changed since the original descriptor decode");
            }
            var child = verifiedProgram.value();
            requirePhysicalBytes(verifiedProgram.javaHome(), child.javaHome(), candidate, deadlineNanos);
            for (int i = 0; i < child.files().size(); i++) {
                requirePhysicalBytes(verifiedProgram.filePaths().get(i), child.files().get(i).path(), candidate,
                        deadlineNanos);
            }
            child.requireCurrentLauncher(recipeId);
            launcherInput.requireRecipe(parsed);
            launcherInput.linuxDeployment().requireProgram(child, recipeId);
            var deployment = launcherInput.linuxDeployment().versionTwo();
            if (!Runtime.version().toString().equals(deployment.provider().runtime())) {
                throw new IOException("Actual runtime build differs from the original provider image binding");
            }
            requireVerifiedRuntimeImage(deadlineNanos);
            List<Path> parsedPaths = List.of(launcherInput.dropper().path(), launcherInput.supervisorDirectory(),
                    launcherInput.manifest(), launcherInput.proof(), launcherInput.policy(), launcherInput.aesKey());
            List<String> originals = launcherSource.paths().values();
            for (int i = 0; i < parsedPaths.size(); i++) {
                requirePhysicalBytes(originals.get(i), parsedPaths.get(i), candidate, deadlineNanos);
            }
            if (!launcherSourceIdentity.equals(SegmentClassicHandoff.digest(launcherInput.encode()))) {
                throw new IOException("Original D2 descriptor text differs from its canonical typed value");
            }
            for (var file : deployment.files()) {
                requirePhysicalBytes(file.path(), candidate.filesystem().getPath(file.path()), candidate,
                        deadlineNanos);
            }
            var loading = deployment.loading();
            requireUtf8LoadingText(loading.interpreter(), deadlineNanos);
            for (var alias : loading.aliases()) {
                requireUtf8LoadingText(alias.link(), deadlineNanos);
                requireUtf8LoadingText(alias.target(), deadlineNanos);
            }
            for (var directory : loading.directories()) {
                requireUtf8LoadingText(directory.path(), deadlineNanos);
            }
            for (var configuration : loading.configuration()) {
                requireUtf8LoadingText(configuration.path(), deadlineNanos);
            }
            for (var context : loading.contexts()) {
                requireUtf8LoadingText(context.logicalLoadName(), deadlineNanos);
            }
            for (String directory : loading.policy().defaultDirectories()) {
                requireUtf8LoadingText(directory, deadlineNanos);
            }
            for (var link : deployment.installLinks()) {
                requireUtf8LoadingText(link.path(), deadlineNanos);
                requireUtf8LoadingText(link.target(), deadlineNanos);
            }
            if (!sameProvider(candidate, linuxProviderObjects())) {
                throw new IOException("Default implementation changed during original byte binding");
            }
            preload.verifyOriginal(deadlineNanos);
            files.requireHealthy();
            requireTime(deadlineNanos);
            linuxProvider = candidate;
        } catch (IOException | RuntimeException | Error problem) {
            linuxProvider = null;
            launcherFailure = problem;
            throw problem;
        } finally {
            initializingThread = null;
        }
    }

    /** Current uname and the actual directory in the launcher mount view. */
    synchronized void verifyLinuxKernelAndCgroupRoot(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            requireTime(deadline);
            var actual = nativeIo.currentKernelIdentity();
            requireLinuxKernelIdentity(deployment.platform(), actual);
            observeCgroupRoot(linuxPhysicalPath(CGROUP_ROOT, deadline), deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Reobserve the same held root across the later startup checks. */
    synchronized void recheckLinuxCgroupRoot(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            originalLinuxDeployment(deadline);
            if (linuxCgroupRoot == null) {
                throw new IOException("Original D2 cgroup directory is not held");
            }
            observeCgroupRoot(linuxPhysicalPath(CGROUP_ROOT, deadline), deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** The test owner uses a real local directory and reports its native filesystem facts. */
    synchronized NativeIO.DirectoryFileSystem readCgroupDirectory(Path directory, long deadline) throws IOException {
        if (production) {
            throw new IOException("Data-only directory reads cannot seed a production owner");
        }
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeCgroupRoot(directory, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private NativeIO.DirectoryFileSystem observeCgroupRoot(Path directory, long deadline) throws IOException {
        requireTime(deadline);
        var held = linuxCgroupRoot;
        if (held != null && !held.path().equals(directory)) {
            throw new IOException("Original cgroup directory path differs");
        }
        var before = cgroupPathIdentities(directory, deadline);
        if (held != null && !held.identities().equals(before)) {
            throw new IOException("Fixed path no longer identifies the original cgroup directory");
        }
        if (production) {
            Path sysFs = directory.getParent();
            verifyCgroupSysfsDirectory(sysFs.getParent(), before.get(2), deadline);
            verifyCgroupSysfsDirectory(sysFs, before.get(1), deadline);
        }
        requireTime(deadline);
        FileChannel channel = held == null
                ? SegmentOwnedFiles.open(files, directory, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)
                : held.channel();
        requireTime(deadline);
        var facts = files.directoryFileSystem(channel, nativeIo);
        if (!matchesDirectoryIdentity(facts, before.get(0)) || held != null && !held.facts().equals(facts)) {
            throw new IOException("Original cgroup directory fd differs from its path or previous observation");
        }
        if (production) {
            requireLinuxCgroupRootType(facts.type());
        }
        if (!before.equals(cgroupPathIdentities(directory, deadline))) {
            throw new IOException("Original cgroup directory path changed during its fd query");
        }
        files.requireHealthy();
        requireTime(deadline);
        if (held == null) {
            linuxCgroupRoot = new HeldCgroupRoot(directory, channel, facts, before);
        }
        return facts;
    }

    private List<Identity> cgroupPathIdentities(Path directory, long deadline) throws IOException {
        requireTime(deadline);
        // Compare leaf to root, excluding directory size, link count and timestamps.
        if (!production) {
            var identities = new ArrayList<Identity>();
            Identity leaf = checked(directory);
            if (!leaf.directory() || (leaf.mode() & 022) != 0) {
                throw new IOException("Local observation requires a protected directory");
            }
            identities.add(leaf.parent());
            identities.addAll(chain(directory));
            return List.copyOf(identities);
        }
        Path sysFs = directory.getParent();
        Path filesystemRoot = directory.getRoot();
        var rootIdentity = protectedCgroupDirectory(filesystemRoot, deadline);
        if (nativeIo.hasExtendedAcl(filesystemRoot.toString())
                || !rootIdentity.equals(protectedCgroupDirectory(filesystemRoot, deadline))) {
            throw new IOException("Additional ACL or changed identity on the fixed cgroup path's filesystem root");
        }
        var sys = protectedCgroupDirectory(sysFs.getParent(), deadline);
        var fs = protectedCgroupDirectory(sysFs, deadline);
        var leaf = protectedCgroupDirectory(directory, deadline);
        return List.of(leaf, fs, sys, rootIdentity);
    }

    private static Identity protectedCgroupDirectory(Path path, long deadline) throws IOException {
        requireTime(deadline);
        var observed = SegmentClassicHandoffPaths.fileIdentity(path);
        if (!observed.directory() || observed.uid() != 0 || (observed.mode() & 022) != 0) {
            throw new IOException("Fixed cgroup path requires a protected root-owned directory: " + path);
        }
        requireTime(deadline);
        return observed.parent();
    }

    private void verifyCgroupSysfsDirectory(Path path, Identity expected, long deadline) throws IOException {
        requireTime(deadline);
        try (FileChannel channel = SegmentOwnedFiles.open(files, path,
                StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            requireTime(deadline);
            requireCgroupSysfsIdentity(path, expected, files.directoryFileSystem(channel, nativeIo));
        }
        files.requireHealthy();
        requireTime(deadline);
    }

    // Only the fixed kernfs parents may use this rule. Each must prove its own filesystem type.
    static void requireCgroupSysfsIdentity(Path path, Identity expected, NativeIO.DirectoryFileSystem facts)
            throws IOException {
        if (facts.type() != SYSFS_TYPE || !matchesDirectoryIdentity(facts, expected)) {
            throw new IOException("Fixed cgroup path parent is not the expected sysfs directory: " + path);
        }
    }

    private static boolean matchesDirectoryIdentity(NativeIO.DirectoryFileSystem facts, Identity path) {
        return facts.identity().device() == path.device() && facts.identity().inode() == path.inode();
    }

    /** Independently cold-read on every call; private first value is only this owner's startup comparison. */
    synchronized void verifyLinuxBootId(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            originalLinuxDeployment(deadline);
            observeLinuxBootId(deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Data-only source/owner failure tests; no caller UUID, pathname, fd or qualification. */
    synchronized UUID readLinuxBootId(long deadline) throws IOException {
        if (production) {
            throw new IOException("Data-only boot ID reads cannot seed a production owner");
        }
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxBootId(deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private UUID observeLinuxBootId(long deadline) throws IOException {
        requireTime(deadline);
        var actual = nativeIo.currentLinuxBootId();
        requireTime(deadline);
        files.requireHealthy();
        if (actual == null || actual.equals(new UUID(0, 0)) || linuxBootId != null && !linuxBootId.equals(actual)) {
            throw new IOException("Current kernel boot ID is missing, zero or changed during original startup");
        }
        requireTime(deadline);
        linuxBootId = actual;
        return actual;
    }

    /** Original source before each cold read; neither PID1 nor current birth implies a systemd qualification. */
    synchronized void verifyLinuxProcessBirth(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            originalLinuxDeployment(deadline);
            observeLinuxProcessBirth(deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Fixture comparison only; cannot seed the first observation of any production owner. */
    synchronized NativeIO.LinuxProcessBirth readLinuxProcessBirth(long deadline) throws IOException {
        if (production) {
            throw new IOException("Data-only process birth reads cannot seed a production owner");
        }
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxProcessBirth(deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private NativeIO.LinuxProcessBirth observeLinuxProcessBirth(long deadline) throws IOException {
        requireTime(deadline);
        var actual = nativeIo.currentLinuxProcessBirth();
        requireTime(deadline);
        files.requireHealthy();
        if (actual == null || linuxProcessBirth != null && !linuxProcessBirth.equals(actual)) {
            throw new IOException("Current/PID1 process birth is missing or changed during original startup");
        }
        requireTime(deadline);
        linuxProcessBirth = actual;
        return actual;
    }

    /** Each check reads this calling thread again; neither matching values nor UID zero prove mount/manager state. */
    synchronized void verifyLinuxInitialNamespaces(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            originalLinuxDeployment(deadline);
            requireTime(deadline);
            var actual = nativeIo.currentLinuxThreadNamespaces();
            requireTime(deadline);
            files.requireHealthy();
            requireInitialLinuxNamespaces(actual);
            requireTime(deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Linux UAPI PID_NS_INIT_INO and USER_NS_INIT_INO; comparison of values alone does not establish a source. */
    static void requireInitialLinuxNamespaces(NativeIO.LinuxThreadNamespaces actual) throws IOException {
        if (actual == null || actual.pidInode() != 0xefff_fffcL || actual.userInode() != 0xefff_fffdL) {
            throw new IOException("Current launcher thread must be in the initial Linux PID and user namespaces");
        }
    }

    /** Called after the final D2 namespace check, while the original launcher still owns startup. */
    synchronized void verifyLinuxSystemdPeer(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            originalLinuxDeployment(deadline);
            requireLinuxSystemdPeer(observeLinuxSystemdSocket(linuxPhysicalPath(SYSTEMD_PRIVATE_SOCKET, deadline),
                    deadline));
            requireTime(deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Isolated socket observation for material fixtures. Production always uses the fixed endpoint. */
    synchronized NativeIO.UnixPeerCredentials readLinuxSystemdSocket(Path path, long deadline) throws IOException {
        if (production) {
            throw new IOException("Data-only socket observations cannot select a production systemd endpoint");
        }
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxSystemdSocket(path, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    static void requireLinuxSystemdPeer(NativeIO.UnixPeerCredentials actual) throws IOException {
        if (actual == null || actual.pid() != 1 || actual.uid() != 0) {
            throw new IOException("Original systemd private socket peer must be PID 1 and UID 0");
        }
    }

    private NativeIO.UnixPeerCredentials observeLinuxSystemdSocket(Path path, long deadline) throws IOException {
        requireTime(deadline);
        Path parent = path.getParent();
        if (!path.isAbsolute() || parent == null || !path.equals(path.normalize())) {
            throw new IOException("Original systemd private socket must have an absolute physical path");
        }
        requireLoadingPathBytes(path, deadline);
        Identity parentBefore = systemdSocketParent(parent);
        var parents = chain(parent);
        Identity before = systemdSocketIdentity(path);
        var peer = readSystemdSocketPeer(UnixDomainSocketAddress.of(path), deadline);
        requireTime(deadline);
        if (!parentBefore.equals(systemdSocketParent(parent)) || !parents.equals(chain(parent))
                || !before.equals(systemdSocketIdentity(path))) {
            throw new IOException("Original systemd private socket or protected parent chain changed");
        }
        files.requireHealthy();
        requireTime(deadline);
        return peer;
    }

    private Identity systemdSocketParent(Path parent) throws IOException {
        Identity identity = checked(parent);
        if (!identity.directory() || (identity.mode() & 022) != 0) {
            throw new IOException("Original systemd private socket parent must be a protected directory");
        }
        return identity.parent();
    }

    private Identity systemdSocketIdentity(Path path) throws IOException {
        // The protected parent chain and the connected peer are checked separately. Do not open the socket as a file.
        Identity identity = SegmentClassicHandoffPaths.loadingIdentity(path);
        if ((identity.mode() & 0177777) != 0140700 || identity.uid() != ownerUid) {
            throw new IOException("Original systemd private endpoint must be a trusted owner's mode 0700 socket");
        }
        return identity;
    }

    private NativeIO.UnixPeerCredentials readSystemdSocketPeer(UnixDomainSocketAddress address, long deadline)
            throws IOException {
        var channelRead = new AuxiliaryRead();
        var selectorRead = new AuxiliaryRead();
        auxiliaryReads.add(channelRead);
        auxiliaryReads.add(selectorRead);
        Throwable failure = null;
        try {
            requireTime(deadline);
            channelRead.openStarted = true;
            SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
            channelRead.resource = channel;
            channelRead.openReturned = true;
            requireTime(deadline);
            selectorRead.openStarted = true;
            Selector selector = Selector.open();
            selectorRead.resource = selector;
            selectorRead.openReturned = true;
            channel.configureBlocking(false).register(selector, 0);
            SegmentClassicLauncherOperationIo.connect(channel, selector, address, deadline);
            return nativeIo.unixPeerCredentials(channel);
        } catch (IOException | RuntimeException | Error problem) {
            failure = problem;
            throw problem;
        } finally {
            // Deregister first so the original channel can close without waiting for a selector.
            try {
                finishAuxiliary(selectorRead, failure);
            } catch (IOException | RuntimeException | Error closeFailure) {
                failure = closeFailure;
                throw closeFailure;
            } finally {
                finishAuxiliary(channelRead, failure);
            }
        }
    }

    static void requireLinuxCgroupRootType(long actual) throws IOException {
        if (actual != 0x63677270L) { // Linux UAPI CGROUP2_SUPER_MAGIC, complete f_type value.
            throw new IOException("Current /sys/fs/cgroup/ filesystem type differs from required cgroup2");
        }
    }

    static void requireLinuxKernelIdentity(SegmentClassicLinuxDeploymentV2.Platform expected,
                                           NativeIO.KernelIdentity actual) throws IOException {
        if (expected == null || actual == null || !actual.sysname().equals("Linux")
                || !actual.machine().equals("x86_64") || !actual.release().equals(expected.kernelRelease())
                || !actual.version().equals(expected.kernelVersion())) {
            throw new IOException("Current process uname differs from the original D2 kernel declaration");
        }
    }

    synchronized void verifyLauncherDropper(SegmentClassicLauncherInputs input, long deadlineNanos)
            throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadlineNanos);
            boolean originalD2 = launcherInput != null && launcherInput.linuxDeployment() != null
                    && launcherInput.linuxDeployment().schema() == 2;
            if (originalD2 || input.linuxDeployment() != null && input.linuxDeployment().schema() == 2) {
                if (input != launcherInput) {
                    throw new IOException("D2 dropper I/O requires this owner's original provider/source binding");
                }
                var deployment = originalLinuxDeployment(deadlineNanos);
                input.requireRecipe(parsed);
                // The original Inputs constructor binds its public dropper path/length/SHA to this fixed member.
                observeLinuxLauncherDropper(deployment.files(), deployment.loading(),
                        deployment.isolation().dropper(), deadlineNanos);
                return;
            }
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

    /** Independent original-table data only; it cannot bind the launcher's source/provider or qualify execution. */
    synchronized LinuxElfMemberObservation readLinuxLauncherDropper(
            List<SegmentClassicLinuxDeploymentV2.File> members, SegmentClassicLinuxDeploymentV2.Loading loading,
            int dropperIndex, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxLauncherDropper(members, loading, dropperIndex, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private LinuxElfMemberObservation observeLinuxLauncherDropper(
            List<SegmentClassicLinuxDeploymentV2.File> declared, SegmentClassicLinuxDeploymentV2.Loading loading,
            int dropperIndex, long deadline) throws IOException {
        var members = fixedLinuxContextMembers(declared, loading, deadline);
        if (dropperIndex < 0 || dropperIndex >= members.size()
                || members.get(dropperIndex).role() != SegmentClassicLinuxDeployment.Role.DROPPER) {
            throw new IOException("Original launcher dropper index must identify its DROPPER member");
        }
        var file = members.get(dropperIndex);
        var metadata = linuxElfDeclaration(loading, dropperIndex);
        var dropper = observeLinuxElfMember(dropperIndex, file, metadata, loading.interpreter(), deadline);
        LinuxLoadPathObservation interpreter = null;
        Map<Path, String> aliases = null;
        if (dropper.elf().interpreter() != null) {
            if (!dropper.elf().interpreter().equals(loading.interpreter())) {
                throw new IOException("Original dropper PT_INTERP differs from the declared interpreter literal");
            }
            aliases = linuxContextAliases(loading, deadline);
            var context = new SegmentClassicLinuxDeploymentV2.Context(loading.loader(), loading.interpreter());
            interpreter = observeLinuxLoadPath(-1, context, members.get(loading.loader()),
                    linuxElfDeclaration(loading, loading.loader()), loading.interpreter(), aliases, deadline);
        }
        Path physical = linuxMaterialPath(file.path(), deadline);
        if (!dropper.identity().equals(checked(physical)) || !dropper.parents().equals(chain(physical))) {
            throw new IOException("Dropper material changed during interpreter verification");
        }
        if (interpreter != null) {
            var terminal = observeLinuxPath(LinuxPathKind.INTERPRETER,
                    loadingNativePath(interpreter.logical(), deadline), aliases, true, deadline);
            if (!interpreter.path().equals(terminal)
                    || !interpreter.content().identity().equals(terminal.identity())) {
                throw new IOException("Original dropper interpreter path changed during verification");
            }
        }
        files.requireHealthy();
        requireTime(deadline);
        return dropper;
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
                || configurationFailure != null || launcherFailure != null
                || diagnosticsInFlight && diagnosticsCaller != Thread.currentThread()
                || backendInFlight && backendCaller != Thread.currentThread()) {
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
        if (initializingThread == Thread.currentThread() || diagnosticsCaller == Thread.currentThread()
                || backendCaller == Thread.currentThread()
                || diagnosticsOutput != null && diagnosticsOutput.isCurrentThread()) {
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
        boolean diagnosticsDrained = diagnosticsOutput == null || diagnosticsOutput.drained();
        return new Drain(result.drained() && auxiliaryReads.isEmpty() && !diagnosticsInFlight && diagnosticsDrained
                        && !backendInFlight,
                initializationFailure, result, auxiliaryReads.size(), auxiliaryCloseFailure, programFailure,
                configurationFailure, launcherFailure, diagnosticsInFlight, diagnosticsStartClaimed,
                diagnosticsDrained, diagnosticsFailure != null || diagnosticsOutput == null
                        ? diagnosticsFailure : diagnosticsOutput.failure(), backendInFlight, backendFailure);
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
        Map<Path, String> aliases = new HashMap<>();
        for (var alias : loading.aliases()) {
            aliases.put(alias.link(), alias.target());
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
            SegmentClassicLinuxDeployment.Loading loading, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            var directories = new ArrayList<LinuxSearchDirectory>();
            for (int i = 0; i < loading.directories().size(); i++) {
                var directory = loading.directories().get(i);
                directories.add(new LinuxSearchDirectory(i, directory.path().toString(), directory.present()));
            }
            Map<Path, String> aliases = new HashMap<>();
            for (var alias : loading.aliases()) {
                aliases.put(alias.link(), alias.target());
            }
            return searchDirectories(directories, aliases, LinuxPathKind.DIRECTORY, deadline).stream()
                    .map(LinuxSearchDirectoryObservation::legacy).toList();
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Whole original D2 table only; no caller directories, aliases, budget or prior observations. */
    synchronized List<LinuxSearchDirectoryObservation> verifyLinuxSearchDirectories(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            var directories = new ArrayList<LinuxSearchDirectory>();
            for (int i = 0; i < deployment.loading().directories().size(); i++) {
                var directory = deployment.loading().directories().get(i);
                directories.add(new LinuxSearchDirectory(i, directory.path(), directory.present()));
            }
            return searchDirectories(directories, originalLinuxAliases(deployment, deadline), LinuxPathKind.LOOKUP,
                    deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Data-only A/B observations; fixture declarations cannot upgrade the typed owner's original source. */
    synchronized List<LinuxSearchDirectoryObservation> readLinuxSearchDirectories(
            List<LinuxSearchDirectory> directories, Map<Path, String> aliases, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return searchDirectories(directories, aliases, LinuxPathKind.LOOKUP, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Shared single-pass data I/O; fixed budget charges cannot confer a typed source or startup qualification. */
    synchronized List<LinuxSearchDirectoryObservation> readLinuxDirectoryPass(List<LinuxSearchDirectory> directories,
            Map<Path, String> aliases, LoadingDirectoryBudget budget, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            var fixed = fixedSearchDirectories(directories, deadline);
            var result = observeLinuxSearchDirectories(fixed, fixedLoadingAliases(aliases, deadline),
                    LinuxPathKind.LOOKUP, budget, deadline);
            files.requireHealthy();
            requireTime(deadline);
            return result;
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private List<LinuxSearchDirectory> fixedSearchDirectories(List<LinuxSearchDirectory> declarations, long deadline)
            throws IOException {
        var directories = List.copyOf(declarations);
        if (directories.size() > 64) {
            throw new IOException("Excessive original search directory declarations");
        }
        Set<Integer> indices = new HashSet<>();
        for (var directory : directories) {
            requireTime(deadline);
            if (!directory.logical().startsWith("/") || !indices.add(directory.directory())) {
                throw new IOException("Search directories require absolute logical paths and unique original indices");
            }
            requireUtf8LoadingText(directory.logical(), deadline);
        }
        return directories;
    }

    private List<LinuxSearchDirectoryObservation> searchDirectories(List<LinuxSearchDirectory> declarations,
            Map<Path, String> aliasDeclarations, LinuxPathKind kind, long deadline) throws IOException {
        var directories = fixedSearchDirectories(declarations, deadline);
        var aliases = fixedLoadingAliases(aliasDeclarations, deadline);
        var first = observeLinuxSearchDirectories(directories, aliases, kind, new LoadingDirectoryBudget(), deadline);
        var second = observeLinuxSearchDirectories(directories, aliases, kind, new LoadingDirectoryBudget(), deadline);
        if (!first.equals(second)) {
            throw new IOException("Original Linux loading directory members changed between observations");
        }
        files.requireHealthy();
        requireTime(deadline);
        return first;
    }

    private List<LinuxSearchDirectoryObservation> observeLinuxSearchDirectories(List<LinuxSearchDirectory> directories,
            Map<Path, String> aliases, LinuxPathKind kind, LoadingDirectoryBudget budget, long deadline)
            throws IOException {
        Map<Path, LinuxDirectoryObservation> observed = new HashMap<>();
        var result = new ArrayList<LinuxSearchDirectoryObservation>();
        for (var directory : directories) {
            Path entry = loadingNativePath(directory.logical(), deadline);
            var path = searchDirectoryPath(directory, entry, aliases, kind, deadline);
            List<String> names = List.of();
            if (path.identity() != null) {
                var cached = observed.get(path.resolved());
                if (cached == null) {
                    names = enumerateLinuxDirectory(path.resolved(), budget, deadline);
                    observed.put(path.resolved(), new LinuxDirectoryObservation(path, names));
                } else {
                    if (!cached.path().identity().equals(path.identity())) {
                        throw new IOException("Previously enumerated physical search directory changed");
                    }
                    names = cached.members();
                }
            }
            if (!path.equals(searchDirectoryPath(directory, entry, aliases, kind, deadline))) {
                throw new IOException("Original loading directory or parent changed during enumeration");
            }
            result.add(new LinuxSearchDirectoryObservation(directory.directory(), directory.logical(),
                    directory.present(), path, names));
        }
        requireTime(deadline);
        return List.copyOf(result);
    }

    private LinuxPathObservation searchDirectoryPath(LinuxSearchDirectory directory, Path entry,
            Map<Path, String> aliases, LinuxPathKind kind, long deadline) throws IOException {
        var path = observeLinuxPath(kind, entry, aliases, directory.present(), deadline);
        if (path.identity() == null ? directory.present() : !directory.present() || !path.identity().directory()
                || (path.identity().mode() & 022) != 0) {
            throw new IOException("Search directory differs from its declared presence, type or protection");
        }
        return path;
    }

    private Path loadingNativePath(String logical, long deadline) throws IOException {
        SegmentClassicLinuxGlob.requireLogicalText(logical);
        requireUtf8LoadingText(logical, deadline);
        // Preserve a raw trailing slash even when the approved Path provider drops it while constructing Path.
        Path path = utf8LoadingPath(logical.endsWith("/") && !logical.equals("/") ? logical + "." : logical);
        requireLoadingPathBytes(path, deadline);
        return path;
    }

    private List<String> enumerateLinuxDirectory(Path directory, LoadingDirectoryBudget budget, long deadline)
            throws IOException {
        var names = new java.util.TreeSet<String>();
        var read = new AuxiliaryRead();
        auxiliaryReads.add(read);
        Throwable failure = null;
        try {
            requireTime(deadline);
            requireLoadingPathBytes(directory, deadline);
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

    private void requireLoadingPathBytes(Path path, long deadline) throws IOException {
        if (linuxProvider != null) {
            if (path.getClass() != linuxProvider.pathClass()) {
                throw new IOException("Loading path has an unapproved implementation");
            }
            requirePhysicalBytes(path.toString(), path, linuxProvider, deadline);
        }
        requireUtf8LoadingPath(path);
        requireTime(deadline);
    }

    private LinuxPathObservation observeLinuxPath(LinuxPathKind kind, Path entry,
            Map<Path, String> aliases, boolean present, long deadline) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (GeneralSecurityException unavailable) {
            throw new IOException("Required loading path SHA-256 is unavailable", unavailable);
        }
        requireTime(deadline);
        requireLoadingPathBytes(entry, deadline);
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
            requireLoadingPathBytes(candidate, deadline);
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
                requireLoadingPathBytes(unresolved, deadline);
                return new LinuxPathObservation(kind, entry, unresolved, candidate, null,
                        new SegmentClassicHandoff.Digest(digest.digest()));
            }
            if ((parent.mode() & 022) != 0 && ((parent.mode() & 01000) == 0
                    || raw.uid() != parent.uid() && raw.uid() != ownerUid)) {
                throw new IOException("Linux loading parent permits replacement by an untrusted principal");
            }
            boolean last = pending.isEmpty();
            if ((raw.mode() & 0170000) == 0120000) {
                if (kind == LinuxPathKind.LITERAL && last) {
                    if (raw.uid() != ownerUid || raw.links() != 1) {
                        throw new IOException("Untrusted final literal loading link");
                    }
                    observeLinuxComponent(digest, candidate, raw);
                    requireTime(deadline);
                    return new LinuxPathObservation(kind, entry, candidate, null, raw,
                            new SegmentClassicHandoff.Digest(digest.digest()));
                }
                var alias = aliases.get(candidate);
                if (alias == null || raw.uid() != ownerUid || raw.links() != 1
                        || !present && last && !lookupKind(kind)) {
                    throw new IOException("Undeclared, untrusted or falsely absent Linux loading alias");
                }
                readLoadingAlias(candidate, raw, alias, deadline);
                observeLinuxComponent(digest, candidate, raw);
                byte[] target = alias.getBytes(StandardCharsets.UTF_8);
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
                if (alias.startsWith("/")) {
                    current = entry.getRoot();
                    parent = checked(current);
                    observeLinuxComponent(digest, current, parent.parent());
                }
                SegmentClassicLinuxDeployment.prependLoadingTarget(pending, alias);
                try {
                    SegmentClassicLinuxDeployment.requireLoadingPathCapacity(current, pending);
                } catch (IllegalArgumentException excessive) {
                    throw new IOException("Expanded Linux loading path exceeds its fixed capacity", excessive);
                }
                continue;
            }
            Identity identity = checked(candidate);
            if (!identity.equals(raw)) {
                throw new IOException("Linux loading component changed during observation");
            }
            if (!last && !identity.directory()) {
                if (lookupKind(kind)) {
                    observeLinuxComponent(digest, candidate, identity);
                    requireTime(deadline);
                    throw new LookupNotDirectory(new LinuxPathObservation(kind, entry, candidate, null, identity,
                            new SegmentClassicHandoff.Digest(digest.digest())));
                }
                throw new IOException("Linux loading component is not a directory");
            }
            observeLinuxComponent(digest, candidate, last ? identity : identity.parent());
            if (last) {
                return linuxLoadingLeaf(kind, entry, candidate, identity, present, digest, deadline);
            }
            current = candidate;
            parent = identity;
        }
        if (lookupKind(kind)) {
            return linuxLoadingLeaf(kind, entry, current, parent, present, digest, deadline);
        }
        throw new IOException("Linux loading entry must name an explicit non-root path");
    }

    private static LinuxPathObservation linuxLoadingLeaf(LinuxPathKind kind, Path entry, Path resolved,
            Identity identity, boolean present, MessageDigest digest, long deadline) throws IOException {
        if (!lookupKind(kind) && (resolved.getParent() == null || !present || kind == LinuxPathKind.ALIAS
                || (kind == LinuxPathKind.DIRECTORY) != identity.directory() || (identity.mode() & 022) != 0)) {
            throw new IOException("Linux loading leaf differs from its presence, kind or protection rule");
        }
        requireTime(deadline);
        return new LinuxPathObservation(kind, entry, resolved, null, identity,
                new SegmentClassicHandoff.Digest(digest.digest()));
    }

    synchronized LinuxConfigurationGraph verifyLinuxConfigurationGraph(SegmentClassicLinuxDeployment.Loading loading,
            List<SegmentClassicLinuxDeployment.File> approved, SegmentClassicLinuxDeployment.Requirement rootEntry,
            long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            if (!loading.configuration().contains(rootEntry) || approved.size() > 1024
                    || rootEntry.file() >= approved.size()
                    || rootEntry.file() >= 0 && approved.get(rootEntry.file()).role()
                    != SegmentClassicLinuxDeployment.Role.LOADER_CONFIG) {
                throw new IOException("Configuration graph root is not its original approved configuration member");
            }
            var materials = new ArrayList<LinuxConfigurationMaterial>();
            Set<Path> paths = new HashSet<>();
            for (int i = 0; i < approved.size(); i++) {
                var file = approved.get(i);
                if (!paths.add(file.path())) {
                    throw new IOException("Duplicate approved configuration graph file path");
                }
                if (file.role() == SegmentClassicLinuxDeployment.Role.LOADER_CONFIG) {
                    materials.add(new LinuxConfigurationMaterial(i, file.path(), file.length(), file.sha256(),
                            ConfigurationKind.LDCONFIG));
                }
            }
            Map<Path, String> aliases = new HashMap<>();
            for (var alias : loading.aliases()) {
                aliases.put(alias.link(), alias.target());
            }
            return configurationGraph(materials, aliases, rootEntry.path(), rootEntry.file(), deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Shared owned graph data only; caller facts cannot establish this owner's typed D2 source/provider state. */
    synchronized LinuxConfigurationGraph readLinuxConfigurationGraph(List<LinuxConfigurationMaterial> materials,
            Map<Path, String> aliases, Path rootPath, int rootFile, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return configurationGraph(materials, aliases, rootPath, rootFile, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private Map<Path, String> fixedLoadingAliases(Map<Path, String> declarations, long deadline) throws IOException {
        var aliases = Map.copyOf(declarations);
        if (aliases.size() > 128) {
            throw new IOException("Excessive original loading aliases");
        }
        for (var alias : aliases.entrySet()) {
            Path link = alias.getKey();
            if (!link.isAbsolute() || link.getParent() == null || !link.equals(link.normalize())) {
                throw new IOException("Loading alias requires its original canonical physical path");
            }
            requireLoadingPathBytes(link, deadline);
            requireUtf8LoadingText(alias.getValue(), deadline);
        }
        return aliases;
    }

    private LinuxConfigurationGraph configurationGraph(List<LinuxConfigurationMaterial> declarations,
            Map<Path, String> aliasDeclarations, Path rootPath, int rootFile, long deadline) throws IOException {
        return configurationGraph(declarations, aliasDeclarations, rootPath, rootFile, false, deadline);
    }

    private LinuxConfigurationGraph configurationGraph(List<LinuxConfigurationMaterial> declarations,
            Map<Path, String> aliasDeclarations, Path rootPath, int rootFile, boolean bindIdentity, long deadline)
            throws IOException {
        requireTime(deadline);
        var materials = List.copyOf(declarations);
        if (materials.size() > 1024 || rootFile < -1 || rootFile >= 1024) {
            throw new IOException("Configuration graph declarations exceed their original bounds");
        }
        Map<Path, LinuxConfigurationMaterial> configuration = new HashMap<>();
        Map<Integer, LinuxConfigurationMaterial> indices = new HashMap<>();
        for (var file : materials) {
            requireTime(deadline);
            if (!file.path().isAbsolute() || !file.path().equals(file.path().normalize())) {
                throw new IOException("Declared graph configuration path is unsupported");
            }
            if (file.kind() == ConfigurationKind.LDCONFIG
                    && file.length() > SegmentClassicLinuxConfiguration.MAX_BYTES) {
                throw new IOException("Declared graph configuration length exceeds its supported bounds");
            }
            var priorPath = configuration.putIfAbsent(file.path(), file);
            var priorIndex = indices.putIfAbsent(file.file(), file);
            if (priorPath != null && !priorPath.equals(file) || priorIndex != null && !priorIndex.equals(file)) {
                throw new IOException("Conflicting physical configuration material, index or kind");
            }
        }
        if (rootFile >= 0 && (indices.get(rootFile) == null
                || indices.get(rootFile).kind() != ConfigurationKind.LDCONFIG)) {
            throw new IOException("Configuration graph root requires its original LDCONFIG material");
        }
        var fixed = Map.copyOf(configuration);
        var aliases = fixedLoadingAliases(aliasDeclarations, deadline);
        var first = new ConfigurationScan(fixed, aliases, bindIdentity, deadline).read(rootPath, rootFile);
        var second = new ConfigurationScan(fixed, aliases, bindIdentity, deadline).read(rootPath, rootFile);
        if (!first.equals(second)) {
            throw new IOException("Complete configuration graph changed between original observations");
        }
        files.requireHealthy();
        requireTime(deadline);
        return first;
    }

    private final class ConfigurationScan {
        private final Map<Path, LinuxConfigurationMaterial> configuration;
        private final Map<Path, String> aliases;
        private final SegmentClassicLinuxConfiguration.ExpansionBudget budget =
                new SegmentClassicLinuxConfiguration.ExpansionBudget();
        private final LoadingDirectoryBudget directoryBudget = new LoadingDirectoryBudget();
        private final Set<ConfigurationContext> active = new HashSet<>();
        private final List<LinuxConfigurationFile> visited = new ArrayList<>();
        private final List<LinuxConfigurationInclude> includes = new ArrayList<>();
        private final List<LinuxDirectoryObservation> directories = new ArrayList<>();
        private final Map<Path, Integer> directoryIndices = new HashMap<>();
        private final MessageDigest observed;
        private final boolean bindIdentity;
        private final long deadline;

        ConfigurationScan(Map<Path, LinuxConfigurationMaterial> configuration, Map<Path, String> aliases,
                          boolean bindIdentity, long deadline) throws IOException {
            this.configuration = configuration;
            this.aliases = aliases;
            this.bindIdentity = bindIdentity;
            this.deadline = deadline;
            try {
                observed = MessageDigest.getInstance("SHA-256");
            } catch (GeneralSecurityException unavailable) {
                throw new IOException("Configuration observation SHA-256 unavailable", unavailable);
            }
        }

        LinuxConfigurationGraph read(Path rootPath, int rootFile) throws IOException {
            if (rootFile == -1) {
                budget.beginFile(1);
                var path = observeLinuxPath(LinuxPathKind.CONFIGURATION, rootPath, aliases, false, deadline);
                remember(rootPath.toString(), path, true);
            } else {
                readFile(rootPath.toString(), rootFile, 1);
            }
            var result = new LinuxConfigurationGraph(rootFile == -1, budget.usage(), visited, includes,
                    directories, new SegmentClassicHandoff.Digest(observed.digest()));
            requireTime(deadline);
            return result;
        }

        private void readFile(String logical, int expected, int depth) throws IOException {
            requireTime(deadline);
            var path = observeLinuxPath(LinuxPathKind.CONFIGURATION, loadingNativePath(logical, deadline), aliases,
                    true, deadline);
            remember(logical, path, true);
            var file = configuration.get(path.resolved());
            if (file == null || expected >= 0 && expected != file.file()) {
                throw new IOException("Included logical source does not resolve to its approved configuration file");
            }
            if (file.kind() != ConfigurationKind.LDCONFIG) {
                throw new IOException("Unsupported configuration kind cannot enter the ldconfig include graph");
            }
            budget.beginFile(depth);
            int index = file.file();
            var context = new ConfigurationContext(path.identity().device(), path.identity().inode(),
                    SegmentClassicLinuxGlob.logicalDirectory(logical));
            if (!active.add(context)) {
                throw new IOException("Active configuration file and logical include context form a cycle");
            }
            try {
                budget.retainFile(file.length());
                requireLoadingPathBytes(file.path(), deadline);
                var material = hashFile(file.path(), file.length(), file.sha256(), false, LinuxContentKind.LDCONFIG,
                        bindIdentity, deadline).legacy();
                if (!path.identity().equals(material.identity())) {
                    throw new IOException("Included configuration identity changed before original hash/parser read");
                }
                for (Identity parent : material.parents()) {
                    rememberText(parent.toString());
                }
                observed.update(file.sha256().value());
                int source = visited.size();
                visited.add(new LinuxConfigurationFile(logical, index, material.identity(), material.configuration()));
                var cursor = material.configuration().cursor(deadline);
                for (var directive = cursor.next(); directive != null; directive = cursor.next()) {
                    if (directive.kind() != SegmentClassicLinuxConfiguration.Kind.INCLUDE) {
                        continue;
                    }
                    budget.pattern();
                    String pattern = directive.value().utf8();
                    String absolute = pattern.startsWith("/") ? pattern
                            : SegmentClassicLinuxGlob.logicalJoin(context.directory(), pattern);
                    var searches = new ArrayList<LinuxConfigurationSearch>();
                    List<String> matches = glob(absolute, searches);
                    includes.add(new LinuxConfigurationInclude(source, directive.line(), directive.column(),
                            directive.word(), pattern, searches, matches));
                    for (String match : matches) {
                        readFile(match, -1, depth + 1);
                    }
                }
            } finally {
                active.remove(context);
            }
        }

        private List<String> glob(String pattern, List<LinuxConfigurationSearch> searches) throws IOException {
            List<SegmentClassicLinuxGlob.Step> plan = SegmentClassicLinuxGlob.plan(pattern, deadline);
            List<String> values = List.of();
            for (int i = plan.size() - 1; i >= 0; i--) {
                requireTime(deadline);
                var step = plan.get(i);
                var next = new ArrayList<String>();
                switch (step.kind()) {
                    case DIRECTORY -> inDirectory(step.directory(), step.component(), step.onlyDirectories(), searches,
                            next);
                    case MATCH -> {
                        for (String directory : values) {
                            inDirectory(directory, step.component(), step.onlyDirectories(), searches, next);
                        }
                    }
                    case MARK -> {
                        for (String value : values) {
                            var path = lookup(value, false);
                            next.add(path.complete() && path.path().identity() != null
                                    && path.path().identity().directory() ? value + "/" : value);
                            SegmentClassicLinuxGlob.requireLogicalText(next.get(next.size() - 1));
                        }
                    }
                    default -> throw new IOException("Unknown include glob plan step");
                }
                values = next;
            }
            values.sort((left, right) -> Arrays.compareUnsigned(left.getBytes(StandardCharsets.UTF_8),
                    right.getBytes(StandardCharsets.UTF_8)));
            var result = List.copyOf(values);
            requireTime(deadline);
            return result;
        }

        private void inDirectory(String logical, String component, boolean onlyDirectories,
                List<LinuxConfigurationSearch> searches, List<String> matches) throws IOException {
            requireTime(deadline);
            var lookup = lookup(logical, false);
            var path = lookup.path();
            if (!lookup.complete() || path.identity() == null || !path.identity().directory()) {
                return;
            }
            if (SegmentClassicLinuxGlob.patternType(component) == 0) {
                searches.add(new LinuxConfigurationSearch(logical, path, -1));
                String candidate = SegmentClassicLinuxGlob.logicalJoin(logical, component);
                var literal = lookup(candidate, true);
                if (literal.complete() && literal.path().identity() != null) {
                    matches.add(candidate);
                }
                return;
            }
            if ((path.identity().mode() & 022) != 0) {
                throw new IOException("Glob enumeration requires a protected nonwritable directory");
            }
            Integer index = directoryIndices.get(path.resolved());
            if (index == null) {
                List<String> names = enumerateLinuxDirectory(path.resolved(), directoryBudget, deadline);
                if (!path.equals(lookup(logical, false).path())) {
                    throw new IOException("Glob directory changed during original enumeration");
                }
                index = directories.size();
                directoryIndices.put(path.resolved(), index);
                directories.add(new LinuxDirectoryObservation(path, names));
            } else if (!directories.get(index).path().identity().equals(path.identity())) {
                throw new IOException("Previously enumerated physical glob directory changed");
            }
            searches.add(new LinuxConfigurationSearch(logical, path, index));
            var matcher = new SegmentClassicLinuxGlob(component);
            var candidates = new ArrayList<>(directories.get(index).members());
            // DirectoryStream omits these fixed lookup candidates; they are never reported as physical members.
            candidates.add(".");
            candidates.add("..");
            for (String member : candidates) {
                requireTime(deadline);
                if (!matcher.matches(member.getBytes(StandardCharsets.UTF_8), deadline)) {
                    continue;
                }
                String candidate = SegmentClassicLinuxGlob.logicalJoin(logical, member);
                if (onlyDirectories) {
                    var directory = lookup(candidate, false);
                    if (!directory.complete() || directory.path().identity() == null
                            || !directory.path().identity().directory()) {
                        continue;
                    }
                }
                matches.add(candidate);
            }
        }

        private LoadingLookup lookup(String logical, boolean literal) throws IOException {
            LinuxPathKind kind = literal ? LinuxPathKind.LITERAL : LinuxPathKind.LOOKUP;
            LoadingLookup result;
            try {
                result = new LoadingLookup(observeLinuxPath(kind, loadingNativePath(logical, deadline), aliases,
                        false, deadline), true);
            } catch (LookupNotDirectory notDirectory) {
                result = new LoadingLookup(notDirectory.observed, false);
            }
            remember(logical, result.path(), result.complete());
            return result;
        }

        private void remember(String logical, LinuxPathObservation path, boolean complete) {
            rememberText(logical);
            rememberText(path.kind().name());
            rememberText(path.entry().toString());
            rememberText(path.resolved().toString());
            rememberText(path.absentAt() == null ? "" : path.absentAt().toString());
            rememberText(path.identity() == null ? "" : path.identity().toString());
            observed.update((byte) (complete ? 1 : 0));
            observed.update(path.chain().value());
        }

        private void rememberText(String text) {
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            observed.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
            observed.update(bytes);
        }
    }

    private static void observeLinuxComponent(MessageDigest digest, Path path, Identity identity) {
        // A same-process comparison, not a persistent/signable encoding. No unbounded parent list is retained.
        for (String value : List.of(path.toString(), identity.toString())) {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
            digest.update(bytes);
        }
    }

    private void readLoadingAlias(Path path, Identity before, String alias,
                                  long deadline) throws IOException {
        requireTime(deadline);
        requireLoadingPathBytes(path, deadline);
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
                || !text.equals(alias)
                || !MessageDigest.isEqual(target, alias.getBytes(StandardCharsets.UTF_8))) {
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
            LinuxContentKind kind = file.role().elf() ? LinuxContentKind.ELF1
                    : file.role() == SegmentClassicLinuxDeployment.Role.LOADER_CACHE ? LinuxContentKind.CACHE
                    : file.role() == SegmentClassicLinuxDeployment.Role.LOADER_CONFIG ? LinuxContentKind.LDCONFIG
                    : LinuxContentKind.NONE;
            var observed = hashFile(file.path(), file.length(), file.sha256(), executable, kind,
                    deadlineNanos).legacy();
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

    /** Only a member index from this original D2 table; no caller Path/File/parser or source permit. */
    synchronized LinuxElfMemberObservation verifyLinuxElfMember(int fileIndex, long deadlineNanos) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadlineNanos);
            var file = selectLinuxElfMember(deployment, fileIndex);
            return observeLinuxElfMember(fileIndex, file, originalLinuxElf(deployment, fileIndex),
                    deployment.loading().interpreter(), deadlineNanos);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private static SegmentClassicLinuxDeploymentV2.Elf originalLinuxElf(SegmentClassicLinuxDeploymentV2 deployment,
            int fileIndex) throws IOException {
        return linuxElfDeclaration(deployment.loading(), fileIndex);
    }

    private static SegmentClassicLinuxDeploymentV2.Elf linuxElfDeclaration(
            SegmentClassicLinuxDeploymentV2.Loading loading,
            int fileIndex) throws IOException {
        return loading.elf().stream().filter(e -> e.file() == fileIndex).findFirst()
                .orElseThrow(() -> new IOException("Original D2 ELF metadata is missing"));
    }

    private Path linuxMaterialPath(String original, long deadline) throws IOException {
        requireUtf8LoadingText(original, deadline);
        if (linuxProvider != null) {
            return linuxPhysicalPath(original, deadline);
        }
        Path path = Path.of(original);
        requireLoadingPathBytes(path, deadline);
        return path;
    }

    private LinuxElfMemberObservation observeLinuxElfMember(int fileIndex, SegmentClassicLinuxDeploymentV2.File file,
            SegmentClassicLinuxDeploymentV2.Elf expected, String interpreter, long deadline) throws IOException {
        if (fileIndex < 0 || fileIndex >= 1024 || !file.role().elf() || expected.file() != fileIndex) {
            throw new IOException("ELF material index, role or metadata reference differs");
        }
        Path path = linuxMaterialPath(file.path(), deadline);
        boolean executable = file.role() != SegmentClassicLinuxDeployment.Role.LIBRARY
                && file.role() != SegmentClassicLinuxDeployment.Role.MODULE;
        var observed = hashFile(path, file.length(), file.sha256(), executable, LinuxContentKind.ELF2, deadline);
        var elf = observed.content().elf2();
        requireLinuxElfMetadata(file.role(), interpreter, expected, elf);
        var result = new LinuxElfMemberObservation(fileIndex, observed.identity(), observed.parents(),
                observed.content().sha256(), elf);
        files.requireHealthy();
        requireTime(deadline);
        return result;
    }

    /** Original context index only; a negative index cannot choose the interpreter. */
    synchronized LinuxLoadPathObservation verifyLinuxLoadContext(int contextIndex, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            var context = selectLinuxLoadContext(deployment, contextIndex);
            return observeOriginalLinuxLoadPath(deployment, contextIndex, context, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Fixed original loading.loader/interpreter only; context -1 is a result marker. */
    synchronized LinuxLoadPathObservation verifyLinuxInterpreter(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            var loading = deployment.loading();
            var context = new SegmentClassicLinuxDeploymentV2.Context(loading.loader(), loading.interpreter());
            return observeOriginalLinuxLoadPath(deployment, -1, context, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private LinuxLoadPathObservation observeOriginalLinuxLoadPath(SegmentClassicLinuxDeploymentV2 deployment,
            int contextIndex, SegmentClassicLinuxDeploymentV2.Context context, long deadline) throws IOException {
        var file = selectLinuxElfMember(deployment, context.file());
        var expected = originalLinuxElf(deployment, context.file());
        requireLinuxLoadReference(contextIndex, context, file, expected);
        return observeLinuxLoadPath(contextIndex, context, file, expected, deployment.loading().interpreter(),
                originalLinuxAliases(deployment, deadline), deadline);
    }

    /** One original self-contained approved loader only; process initialization/waits never hold this monitor. */
    LinuxLoaderDiagnosticsObservation verifyLinuxLoaderDiagnostics(long deadline) throws IOException {
        final SegmentClassicSystemdQuery.Output output;
        synchronized (this) {
            requireReady();
            requireBoundDeadline(deadline);
            requireTime(deadline);
            if (diagnosticsAttempted) {
                throw new IOException("Original loader diagnostics starts once");
            }
            diagnosticsAttempted = true;
            try {
                output = new SegmentClassicSystemdQuery.Output();
            } catch (RuntimeException | Error failure) {
                diagnosticsFailure = failure;
                launcherFailure = failure;
                throw failure;
            }
            diagnosticsOutput = output;
            diagnosticsInFlight = true;
            diagnosticsCaller = Thread.currentThread();
        }
        try {
            final SegmentClassicLinuxDeploymentV2 deployment;
            final SegmentClassicLinuxDeploymentV2.Context context;
            final LinuxLoadPathObservation before;
            final List<String> command;
            synchronized (this) {
                requireReady();
                initializingThread = Thread.currentThread();
                try {
                    deployment = originalLinuxDeployment(deadline);
                    var program = verifiedProgram.value();
                    if (!program.os().equals("linux") || !program.architecture().equals("x86_64")) {
                        throw new IOException("Unsupported approved platform for loader diagnostics");
                    }
                    requireLinuxDiagnosticsProcessSource();
                    var loading = deployment.loading();
                    context = new SegmentClassicLinuxDeploymentV2.Context(loading.loader(), loading.interpreter());
                    before = observeOriginalLinuxLoadPath(deployment, -1, context, deadline);
                    command = linuxLoaderDiagnosticsCommand(before.logical(), before.content().elf(), deadline);
                } finally {
                    initializingThread = null;
                }
            }
            var builder = new ProcessBuilder(command);
            builder.directory(new java.io.File("/"));
            synchronized (this) {
                requireReady();
                requireTime(deadline);
                diagnosticsStartClaimed = true;
            }
            output.initialize(builder, deadline);
            var bytes = output.read(deadline);
            if (bytes.stderr().length != 0) {
                throw new IOException("Original loader diagnostics produced stderr");
            }
            synchronized (this) {
                requireReady();
                initializingThread = Thread.currentThread();
                try {
                    if (originalLinuxDeployment(deadline) != deployment) {
                        throw new IOException("Original loader deployment changed during diagnostics");
                    }
                    var after = observeOriginalLinuxLoadPath(deployment, -1, context, deadline);
                    requireLinuxDiagnosticsBinding(before, after);
                    files.requireHealthy();
                    requireTime(deadline);
                    return new LinuxLoaderDiagnosticsObservation(after, bytes.stdout());
                } finally {
                    initializingThread = null;
                }
            }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) {
                if (diagnosticsFailure == null) {
                    diagnosticsFailure = output.failure() == null ? failure : output.failure();
                }
                if (launcherFailure == null) {
                    launcherFailure = failure;
                }
            }
            throw failure;
        } finally {
            synchronized (this) {
                diagnosticsInFlight = false;
                diagnosticsCaller = null;
            }
        }
    }

    /** Original JDK bytes are approved, but its process backend and external helper dependencies are not bound yet. */
    static void requireLinuxDiagnosticsProcessSource() throws IOException {
        throw new IOException("Unsupported original JDK process startup chain for loader diagnostics");
    }

    /** Pure fixed-command data only; these checks do not establish approval or permission to execute. */
    static List<String> linuxLoaderDiagnosticsCommand(String interpreter, SegmentClassicLinuxElf.Image2 elf,
            long deadline) throws IOException {
        requireTime(deadline);
        SegmentClassicLinuxGlob.requireLogicalText(interpreter);
        if (!isLinuxSelfContainedLoader(elf)) {
            throw new IOException("Unsupported loader diagnostics requires an approved self-contained ET_DYN loader");
        }
        requireTime(deadline);
        return List.of(interpreter, "--list-diagnostics");
    }

    private static boolean isLinuxSelfContainedLoader(SegmentClassicLinuxElf.Image2 elf) {
        return elf.type() == 3 && elf.entry() != 0 && elf.interpreter() == null && elf.needed().isEmpty();
    }

    /** Original pre/post data comparison only; caller observations cannot construct a bound diagnostic result. */
    static void requireLinuxDiagnosticsBinding(LinuxLoadPathObservation before, LinuxLoadPathObservation after)
            throws IOException {
        if (before == null || !before.equals(after)) {
            throw new IOException("Original loader path or owned ELF identity changed during diagnostics");
        }
    }

    /** Each check reads the current libc table again. An empty sample does not bind an exec environment. */
    synchronized void verifyLinuxNativeEnvironment(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            originalLinuxDeployment(deadline);
            requireTime(deadline);
            var sample = nativeIo.currentLinuxNativeEnvironment();
            requireTime(deadline);
            files.requireHealthy();
            requireEmptyLinuxNativeEnvironment(sample);
            requireTime(deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    static void requireEmptyLinuxNativeEnvironment(NativeIO.NativeEnvironmentSample sample) throws IOException {
        if (sample != NativeIO.NativeEnvironmentSample.EMPTY_SAMPLED) {
            throw new IOException("Original launcher requires an empty Linux native environment sample");
        }
    }

    /** Fixed JNI initialization executes outside this monitor; late return cannot publish after close or expiry. */
    LinuxJdkProcessObservation verifyLinuxJdkProcess(long deadline) throws IOException {
        final SegmentClassicLaunchProgram.Decoded program;
        final SegmentClassicLinuxDeploymentV2 deployment;
        final LinuxFileObservation image;
        final LinuxProvider provider;
        final SegmentClassicLauncherInputs.Decoded source;
        final SegmentClassicHandoff.Digest sourceIdentity;
        final NativeIOBootstrap.Library library;
        final Class<?> processType;
        final Class<?> mechanismType;
        synchronized (this) {
            requireReady();
            requireBoundDeadline(deadline);
            requireTime(deadline);
            if (backendAttempted) {
                throw new IOException("Original cached JDK process observation is one-use");
            }
            backendAttempted = true;
            initializingThread = Thread.currentThread();
            try {
                deployment = originalLinuxDeployment(deadline);
                program = verifiedProgram;
                image = verifiedRuntimeImage;
                provider = linuxProvider;
                source = launcherSource;
                sourceIdentity = launcherSourceIdentity;
                library = NativeIOBootstrap.controlledLibrary();
                processType = Class.forName("java.lang.ProcessImpl", false, null);
                mechanismType = Class.forName("java.lang.ProcessImpl$LaunchMechanism", false, null);
                requireBootstrapType(processType, "java.lang.ProcessImpl");
                requireBootstrapType(mechanismType, "java.lang.ProcessImpl$LaunchMechanism");
                if (!mechanismType.isEnum()) {
                    throw new IOException("Unsupported fixed ProcessImpl mechanism type");
                }
                requireVerifiedRuntimeImage(deadline);
                preload.verifyOriginal(deadline);
                files.requireHealthy();
                requireTime(deadline);
                backendCaller = Thread.currentThread();
                backendInFlight = true;
            } catch (ClassNotFoundException unavailable) {
                var failure = new IOException("Original bootstrap ProcessImpl implementation is unavailable",
                        unavailable);
                backendFailure = failure;
                if (launcherFailure == null) {
                    launcherFailure = failure;
                }
                throw failure;
            } catch (IOException | RuntimeException | Error failure) {
                backendFailure = failure;
                if (launcherFailure == null) {
                    launcherFailure = failure;
                }
                throw failure;
            } finally {
                initializingThread = null;
            }
        }
        try {
            var cached = nativeIo.jdkProcessLaunch();
            synchronized (this) {
                requireReady();
                initializingThread = Thread.currentThread();
                try {
                    if (originalLinuxDeployment(deadline) != deployment || verifiedProgram != program
                            || verifiedRuntimeImage != image || linuxProvider != provider || launcherSource != source
                            || launcherSourceIdentity != sourceIdentity
                            || NativeIOBootstrap.controlledLibrary() != library) {
                        throw new IOException("Original JDK process source changed during its cached observation");
                    }
                    requireBootstrapType(processType, "java.lang.ProcessImpl");
                    requireBootstrapType(mechanismType, "java.lang.ProcessImpl$LaunchMechanism");
                    requireVerifiedRuntimeImage(deadline);
                    preload.verifyOriginal(deadline);
                    var members = fixedLinuxContextMembers(deployment.files(), deployment.loading(), deadline);
                    var selected = bindLinuxJdkHelperCache(program.javaHome(), cached.helperPath(),
                            program.value().files(), members, deployment.loading(), deadline);
                    var helper = observeSelectedLinuxJdkHelper(selected, members, deployment.loading(), deadline);
                    requireVerifiedRuntimeImage(deadline);
                    preload.verifyOriginal(deadline);
                    if (originalLinuxDeployment(deadline) != deployment
                            || NativeIOBootstrap.controlledLibrary() != library) {
                        throw new IOException("Original JDK process source changed during helper association");
                    }
                    files.requireHealthy();
                    requireTime(deadline);
                    var result = new LinuxJdkProcessObservation(cached, helper);
                    backendObservation = result;
                    return result;
                } finally {
                    initializingThread = null;
                }
            }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (this) {
                if (backendFailure == null) {
                    backendFailure = failure;
                }
                if (launcherFailure == null) {
                    launcherFailure = failure;
                }
            }
            throw failure;
        } finally {
            synchronized (this) {
                backendInFlight = false;
                backendCaller = null;
            }
        }
    }

    private void requireVerifiedRuntimeImage(long deadline) throws IOException {
        requireTime(deadline);
        var child = verifiedProgram.value();
        var image = child.files().stream().filter(f -> f.path().equals(child.javaHome().resolve("lib/modules")))
                .findFirst().orElseThrow(() -> new IOException("Original runtime image is missing"));
        if (!verifiedRuntimeImage.identity().equals(checked(image.path()))
                || !verifiedRuntimeImage.parents().equals(chain(image.path()))) {
            throw new IOException("Original verified runtime image identity or protected parents changed");
        }
        requireTime(deadline);
    }

    /** Pure byte/declaration association only; caller data cannot establish owner source or backend permission. */
    static LinuxJdkHelperReference bindLinuxJdkHelperCache(String javaHome, byte[] helperPath,
            List<SegmentClassicLaunchProgram.File> program, List<SegmentClassicLinuxDeploymentV2.File> members,
            SegmentClassicLinuxDeploymentV2.Loading loading, long deadline) throws IOException {
        requireTime(deadline);
        if (helperPath == null || helperPath.length < 2 || helperPath.length > 4097) {
            throw new IOException("Invalid bounded cached JDK helper bytes");
        }
        byte[] actual = helperPath.clone();
        var selected = selectLinuxJdkHelper(javaHome, program, members, loading, deadline);
        byte[] path = selected.logical().getBytes(StandardCharsets.UTF_8);
        byte[] expected = Arrays.copyOf(path, path.length + 1);
        if (!Arrays.equals(expected, actual)) {
            throw new IOException("Cached JDK helper bytes differ from the exact original approved helper path");
        }
        requireTime(deadline);
        return selected;
    }

    /** Only this owner's original raw JDK root and declarations; not an observation of the chosen backend. */
    synchronized LinuxJdkHelperObservation verifyLinuxJdkHelper(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            return observeLinuxJdkHelper(verifiedProgram.javaHome(), verifiedProgram.value().files(),
                    deployment.files(), deployment.loading(), deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Only this owner's actual cached helper can select the direct NEEDED candidate observations. */
    synchronized void verifyLinuxJdkHelperNeededCandidates(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            if (backendInFlight || backendObservation == null) {
                throw new IOException("Original cached JDK helper is required for its NEEDED candidate observations");
            }
            var loading = deployment.loading();
            var members = fixedLinuxContextMembers(deployment.files(), loading, deadline);
            var bound = backendObservation.helper();
            var selected = selectLinuxJdkHelper(verifiedProgram.javaHome(), verifiedProgram.value().files(),
                    members, loading, deadline);
            if (!selected.equals(bound.declaration())) {
                throw new IOException("Original cached JDK helper declaration changed before its NEEDED reads");
            }
            int cacheRule = bound.helper().content().elf().needed().isEmpty() ? -1 : fixedLinuxCacheRule(loading);
            observeLinuxJdkHelperNeededCandidates(members, loading, bound.helper(), bound.interpreter(),
                    cacheRule, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Isolated declaration subtree only. The helper and interpreter are read by this owner. */
    synchronized List<Integer> readLinuxJdkHelperNeededCandidates(
            List<SegmentClassicLinuxDeploymentV2.File> declared, SegmentClassicLinuxDeploymentV2.Loading loading,
            int helperContext, int cacheRule, long deadline) throws IOException {
        if (production) {
            throw new IOException("Data-only JDK helper candidate observations cannot select a production requester");
        }
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            var members = fixedLinuxContextMembers(declared, loading, deadline);
            if (helperContext < 0 || helperContext >= loading.contexts().size()) {
                throw new IOException("JDK helper cache context is outside its declaration table");
            }
            var context = loading.contexts().get(helperContext);
            var member = members.get(context.file());
            if (member.role() != SegmentClassicLinuxDeployment.Role.START_CHAIN
                    || !member.path().endsWith("/lib/jspawnhelper")
                    || !member.path().equals(context.logicalLoadName())) {
                throw new IOException("JDK helper cache fixture requires an exact helper execution context");
            }
            var aliases = linuxContextAliases(loading, deadline);
            var helper = observeLinuxLoadPath(helperContext, context, member,
                    linuxElfDeclaration(loading, context.file()), loading.interpreter(), aliases, deadline);
            var interpreter = observeLinuxJdkHelperInterpreter(helper, members, loading, aliases, deadline);
            return observeLinuxJdkHelperNeededCandidates(members, loading, helper, interpreter, cacheRule, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private List<Integer> observeLinuxJdkHelperNeededCandidates(
            List<SegmentClassicLinuxDeploymentV2.File> members, SegmentClassicLinuxDeploymentV2.Loading loading,
            LinuxLoadPathObservation helper, LinuxLoadPathObservation interpreter, int cacheRule, long deadline)
            throws IOException {
        var aliases = linuxContextAliases(loading, deadline);
        recheckLinuxJdkHelperMaterial(helper, interpreter, aliases, deadline);
        if (interpreter != null && !isLinuxSelfContainedLoader(interpreter.content().elf())) {
            throw new IOException("Unsupported JDK helper PT_INTERP requires an approved self-contained ET_DYN loader");
        }
        var completedRules = new ArrayList<Integer>();
        for (int i = 0; i < loading.needed().size(); i++) {
            requireTime(deadline);
            if (loading.needed().get(i).requester() == helper.context()) {
                // Keep only this requester's observation across the stages, not either parsed cache image.
                var requester = observeLinuxNeededCache(members, loading, i, cacheRule, helper, deadline).requester();
                var tag = planLinuxNeededRequesterTag(loading, i, deadline);
                recheckLinuxJdkHelperMaterial(requester, null, aliases, deadline);
                observeLinuxRequesterTagDirectories(members, loading, i, requester, tag, aliases, null, deadline);
                var plan = planLinuxNeededDefaults(loading, i, deadline);
                recheckLinuxJdkHelperMaterial(requester, null, aliases, deadline);
                observeLinuxDefaultDirectories(members, loading, i, requester, plan, aliases, null, deadline);
                completedRules.add(i);
            }
        }
        recheckLinuxJdkHelperMaterial(helper, interpreter, aliases, deadline);
        files.requireHealthy();
        requireTime(deadline);
        return List.copyOf(completedRules);
    }

    /** Original unit files only; successful disk observations do not establish manager effective configuration. */
    synchronized List<LinuxUnitFileObservation> verifyLinuxUnitFiles(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            return observeLinuxUnitFiles(deployment.files(), deployment.units(), deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Data-only unit references; cannot confer installed provenance or query the systemd manager. */
    synchronized List<LinuxUnitFileObservation> readLinuxUnitFiles(
            List<SegmentClassicLinuxDeploymentV2.File> declarations,
            List<SegmentClassicLinuxDeploymentV2.Unit> units, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxUnitFiles(declarations, units, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private List<LinuxUnitFileObservation> observeLinuxUnitFiles(
            List<SegmentClassicLinuxDeploymentV2.File> declarations,
            List<SegmentClassicLinuxDeploymentV2.Unit> declaredUnits, long deadline) throws IOException {
        requireTime(deadline);
        var members = List.copyOf(declarations);
        var units = List.copyOf(declaredUnits);
        if (members.isEmpty() || members.size() > 1024 || units.isEmpty() || units.size() > 32) {
            throw new IOException("Unit material tables exceed their original D2 bounds");
        }
        try {
            SegmentClassicLinuxDeploymentV2.requireUnitFileReferences(members, units);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Unit file references differ from their original roles or indices", invalid);
        }
        var result = new ArrayList<LinuxUnitFileObservation>();
        for (int i = 0; i < units.size(); i++) {
            var unit = units.get(i);
            result.add(observeLinuxUnitFile(i, -1, unit.fragment(), members, deadline));
            for (int j = 0; j < unit.dropIns().size(); j++) {
                result.add(observeLinuxUnitFile(i, j, unit.dropIns().get(j), members, deadline));
            }
        }
        files.requireHealthy();
        requireTime(deadline);
        return List.copyOf(result);
    }

    private LinuxUnitFileObservation observeLinuxUnitFile(int unit, int dropIn, int member,
            List<SegmentClassicLinuxDeploymentV2.File> members, long deadline) throws IOException {
        var file = members.get(member);
        Path path = linuxMaterialPath(file.path(), deadline);
        var observed = hashFile(path, file.length(), file.sha256(), false, LinuxContentKind.NONE, true, deadline);
        return new LinuxUnitFileObservation(unit, dropIn, member, path, observed.content().sha256(), observed.legacy());
    }

    /** Only original D2 install links; disk facts cannot establish effective manager aliases or dependencies. */
    synchronized List<LinuxInstallLinkObservation> verifyLinuxInstallLinks(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            return observeLinuxInstallLinks(deployment.units(), deployment.installLinks(), deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Data-only declarations share the reader; cannot confer installed provenance or a manager observation. */
    synchronized List<LinuxInstallLinkObservation> readLinuxInstallLinks(
            List<SegmentClassicLinuxDeploymentV2.Unit> units,
            List<SegmentClassicLinuxDeploymentV2.InstallLink> links, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxInstallLinks(units, links, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private List<LinuxInstallLinkObservation> observeLinuxInstallLinks(
            List<SegmentClassicLinuxDeploymentV2.Unit> declaredUnits,
            List<SegmentClassicLinuxDeploymentV2.InstallLink> declarations, long deadline) throws IOException {
        requireTime(deadline);
        var units = List.copyOf(declaredUnits);
        var links = List.copyOf(declarations);
        if (units.isEmpty() || units.size() > 32 || links.size() > 1024) {
            throw new IOException("Install link tables exceed their original D2 bounds");
        }
        try {
            SegmentClassicLinuxDeploymentV2.requireInstallLinkReferences(units, links);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Install link relationships differ from their original unit declarations", invalid);
        }
        var result = new ArrayList<LinuxInstallLinkObservation>();
        for (int i = 0; i < links.size(); i++) {
            var link = links.get(i);
            Path path = linuxMaterialPath(link.path(), deadline);
            requireUtf8LoadingText(link.target(), deadline);
            var leafOnly = Map.of(path, link.target());
            Path parent = path.getParent();
            var parentIdentity = checked(parent).parent();
            var parents = chain(parent);
            var before = observeLinuxPath(LinuxPathKind.ALIAS, path, leafOnly, true, deadline);
            if (!parentIdentity.equals(checked(parent).parent()) || !parents.equals(chain(parent))
                    || !before.identity().equals(SegmentClassicHandoffPaths.loadingIdentity(path))) {
                throw new IOException("Install link or its protected parent chain changed during observation");
            }
            result.add(new LinuxInstallLinkObservation(i, link, before));
        }
        files.requireHealthy();
        requireTime(deadline);
        return List.copyOf(result);
    }

    /** Independent member/subgraph data only; does not establish a complete program, typed source or backend. */
    synchronized LinuxJdkHelperObservation readLinuxJdkHelper(String javaHome,
            List<SegmentClassicLaunchProgram.File> program, List<SegmentClassicLinuxDeploymentV2.File> members,
            SegmentClassicLinuxDeploymentV2.Loading loading, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxJdkHelper(javaHome, program, members, loading, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private LinuxJdkHelperObservation observeLinuxJdkHelper(String javaHome,
            List<SegmentClassicLaunchProgram.File> program, List<SegmentClassicLinuxDeploymentV2.File> declarations,
            SegmentClassicLinuxDeploymentV2.Loading loading, long deadline) throws IOException {
        var members = fixedLinuxContextMembers(declarations, loading, deadline);
        var selected = selectLinuxJdkHelper(javaHome, program, members, loading, deadline);
        return observeSelectedLinuxJdkHelper(selected, members, loading, deadline);
    }

    private LinuxJdkHelperObservation observeSelectedLinuxJdkHelper(LinuxJdkHelperReference selected,
            List<SegmentClassicLinuxDeploymentV2.File> members, SegmentClassicLinuxDeploymentV2.Loading loading,
            long deadline) throws IOException {
        var aliases = linuxContextAliases(loading, deadline);
        var context = loading.contexts().get(selected.context());
        var helper = observeLinuxLoadPath(selected.context(), context, members.get(selected.member()),
                linuxElfDeclaration(loading, selected.member()), loading.interpreter(), aliases, deadline);
        var interpreter = observeLinuxJdkHelperInterpreter(helper, members, loading, aliases, deadline);
        recheckLinuxJdkHelperMaterial(helper, interpreter, aliases, deadline);
        files.requireHealthy();
        requireTime(deadline);
        return new LinuxJdkHelperObservation(selected, helper, interpreter);
    }

    private LinuxLoadPathObservation observeLinuxJdkHelperInterpreter(LinuxLoadPathObservation helper,
            List<SegmentClassicLinuxDeploymentV2.File> members, SegmentClassicLinuxDeploymentV2.Loading loading,
            Map<Path, String> aliases, long deadline) throws IOException {
        String actual = helper.content().elf().interpreter();
        if (actual == null) {
            return null;
        }
        if (!actual.equals(loading.interpreter())) {
            throw new IOException("JDK helper PT_INTERP differs from the exact original interpreter name");
        }
        var fixed = new SegmentClassicLinuxDeploymentV2.Context(loading.loader(), loading.interpreter());
        return observeLinuxLoadPath(-1, fixed, members.get(loading.loader()),
                linuxElfDeclaration(loading, loading.loader()), loading.interpreter(), aliases, deadline);
    }

    private void recheckLinuxJdkHelperMaterial(LinuxLoadPathObservation helper, LinuxLoadPathObservation interpreter,
            Map<Path, String> aliases, long deadline) throws IOException {
        for (var observed : interpreter == null ? List.of(helper) : List.of(helper, interpreter)) {
            var kind = observed.context() == -1 ? LinuxPathKind.INTERPRETER : LinuxPathKind.LOOKUP;
            var after = observeLinuxPath(kind, loadingNativePath(observed.logical(), deadline), aliases, true,
                    deadline);
            if (!observed.path().equals(after) || !observed.content().identity().equals(after.identity())) {
                throw new IOException("JDK helper or interpreter changed during their material association");
            }
        }
    }

    /** Bounded pure matching only; the caller must separately validate the original context subgraph and source. */
    static LinuxJdkHelperReference selectLinuxJdkHelper(String javaHome, List<SegmentClassicLaunchProgram.File> program,
            List<SegmentClassicLinuxDeploymentV2.File> members, SegmentClassicLinuxDeploymentV2.Loading loading,
            long deadline) throws IOException {
        requireTime(deadline);
        try {
            SegmentClassicLinuxDeploymentV2.physical(javaHome);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Fixed JDK helper requires the original canonical raw javaHome", invalid);
        }
        String suffix = "/lib/jspawnhelper";
        if (javaHome.getBytes(StandardCharsets.UTF_8).length > 4096 - suffix.length()
                || program.size() > SegmentClassicLaunchProgram.MAX_FILES || members.size() > 1024) {
            throw new IOException("Fixed JDK helper declaration exceeds its original bounds");
        }
        var fixedProgram = List.copyOf(program);
        var fixedMembers = List.copyOf(members);
        String logical = javaHome + suffix;
        int programIndex = -1;
        for (int i = 0; i < fixedProgram.size(); i++) {
            requireTime(deadline);
            var file = fixedProgram.get(i);
            if (file.path().toString().equals(logical)) {
                if (programIndex != -1 || (file.roles() & SegmentClassicLaunchProgram.JDK) == 0) {
                    throw new IOException("Conflicting or non-JDK fixed helper program declaration");
                }
                programIndex = i;
            }
        }
        int member = -1;
        for (int i = 0; i < fixedMembers.size(); i++) {
            requireTime(deadline);
            var file = fixedMembers.get(i);
            if (file.path().equals(logical)) {
                if (member != -1 || file.role() != SegmentClassicLinuxDeployment.Role.START_CHAIN) {
                    throw new IOException("Unsupported fixed JDK helper requires one original START_CHAIN member");
                }
                member = i;
            }
        }
        if (programIndex == -1 || member == -1) {
            throw new IOException("Unsupported fixed JDK helper lacks its original program or D2 declaration");
        }
        var original = fixedProgram.get(programIndex);
        var file = fixedMembers.get(member);
        if (original.length() != file.length() || !original.sha256().equals(file.sha256())) {
            throw new IOException("Original program and D2 approve conflicting fixed JDK helper bytes");
        }
        int context = -1;
        for (int i = 0; i < loading.contexts().size(); i++) {
            requireTime(deadline);
            var entry = loading.contexts().get(i);
            if (entry.file() == member && entry.logicalLoadName().equals(logical)) {
                if (context != -1) {
                    throw new IOException("Conflicting fixed JDK helper execution contexts");
                }
                context = i;
            }
        }
        if (context == -1) {
            throw new IOException("Unsupported fixed JDK helper lacks its exact original execution context");
        }
        requireTime(deadline);
        return new LinuxJdkHelperReference(programIndex, member, context, logical);
    }

    /** Independent declaration data only; results cannot establish this owner's original D2 source or readiness. */
    synchronized LinuxLoadPathObservation readLinuxLoadPath(int contextIndex,
            SegmentClassicLinuxDeploymentV2.Context context, SegmentClassicLinuxDeploymentV2.File file,
            SegmentClassicLinuxDeploymentV2.Elf expected, String interpreter, Map<Path, String> aliases, long deadline)
            throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxLoadPath(contextIndex, context, file, expected, interpreter, aliases, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private static void requireLinuxLoadReference(int contextIndex, SegmentClassicLinuxDeploymentV2.Context context,
            SegmentClassicLinuxDeploymentV2.File file, SegmentClassicLinuxDeploymentV2.Elf expected)
            throws IOException {
        if (contextIndex < -1 || contextIndex >= 4096 || context.file() < 0 || context.file() >= 1024
                || expected.file() != context.file() || !file.role().elf()
                || contextIndex == -1 && file.role() != SegmentClassicLinuxDeployment.Role.LOADER) {
            throw new IOException("Load path context, ELF member role or metadata reference differs");
        }
    }

    private LinuxLoadPathObservation observeLinuxLoadPath(int contextIndex,
            SegmentClassicLinuxDeploymentV2.Context context,
            SegmentClassicLinuxDeploymentV2.File file, SegmentClassicLinuxDeploymentV2.Elf expected, String interpreter,
            Map<Path, String> aliasDeclarations, long deadline) throws IOException {
        requireTime(deadline);
        requireLinuxLoadReference(contextIndex, context, file, expected);
        var aliases = fixedLoadingAliases(aliasDeclarations, deadline);
        Path entry = loadingNativePath(context.logicalLoadName(), deadline);
        Path physical = linuxMaterialPath(file.path(), deadline);
        LinuxPathKind kind = contextIndex == -1 ? LinuxPathKind.INTERPRETER : LinuxPathKind.LOOKUP;
        var before = observeLinuxPath(kind, entry, aliases, true, deadline);
        if (before.identity() == null || before.identity().directory() || !before.resolved().equals(physical)) {
            throw new IOException("Logical load path differs from its declared physical ELF member");
        }
        var member = observeLinuxElfMember(context.file(), file, expected, interpreter, deadline);
        var after = observeLinuxPath(kind, entry, aliases, true, deadline);
        if (!before.equals(after) || !before.identity().equals(member.identity())
                || !after.identity().equals(member.identity())) {
            throw new IOException("Load path, alias chain or member identity changed across the original ELF read");
        }
        var result = new LinuxLoadPathObservation(contextIndex, context.file(), context.logicalLoadName(), before,
                member);
        files.requireHealthy();
        requireTime(deadline);
        return result;
    }

    /** Pure bounded original table selection only. Source/provider checks are still required before I/O. */
    static SegmentClassicLinuxDeploymentV2.Context selectLinuxLoadContext(SegmentClassicLinuxDeploymentV2 deployment,
            int contextIndex) throws IOException {
        if (contextIndex < 0 || contextIndex >= deployment.loading().contexts().size()) {
            throw new IOException("D2 context index is outside the original context table");
        }
        var context = deployment.loading().contexts().get(contextIndex);
        selectLinuxElfMember(deployment, context.file());
        return context;
    }

    /** Whole original candidate context set only; never accepts a caller observation or a selected winner. */
    synchronized LinuxCandidateContextObservation verifyLinuxCandidateContexts(long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            return observeLinuxCandidateContexts(deployment.files(), deployment.loading(), deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Independent context-subgraph data only; it cannot establish a complete D2 deployment or typed source. */
    synchronized LinuxCandidateContextObservation readLinuxCandidateContexts(
            List<SegmentClassicLinuxDeploymentV2.File> files,
            SegmentClassicLinuxDeploymentV2.Loading loading, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxCandidateContexts(files, loading, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private LinuxCandidateContextObservation observeLinuxCandidateContexts(
            List<SegmentClassicLinuxDeploymentV2.File> declarations, SegmentClassicLinuxDeploymentV2.Loading loading,
            long deadline) throws IOException {
        var members = fixedLinuxContextMembers(declarations, loading, deadline);
        var aliases = linuxContextAliases(loading, deadline);
        var contexts = new ArrayList<LinuxLoadPathObservation>();
        for (int i = 0; i < loading.contexts().size(); i++) {
            requireTime(deadline);
            var context = loading.contexts().get(i);
            var file = members.get(context.file());
            contexts.add(observeLinuxLoadPath(i, context, file, linuxElfDeclaration(loading, context.file()),
                    loading.interpreter(), aliases, deadline));
        }
        var interpreter = new SegmentClassicLinuxDeploymentV2.Context(loading.loader(), loading.interpreter());
        var loader = observeLinuxLoadPath(-1, interpreter, members.get(loading.loader()),
                linuxElfDeclaration(loading, loading.loader()), loading.interpreter(), aliases, deadline);
        var needed = new ArrayList<LinuxNeededContextObservation>();
        for (var rule : loading.needed()) {
            requireTime(deadline);
            String name = contexts.get(rule.requester()).content().elf().needed().get(rule.ordinal());
            needed.add(new LinuxNeededContextObservation(rule, name));
        }
        var result = new LinuxCandidateContextObservation(contexts, loader, needed, loading.modules());
        files.requireHealthy();
        requireTime(deadline);
        return result;
    }

    private List<SegmentClassicLinuxDeploymentV2.File> fixedLinuxContextMembers(
            List<SegmentClassicLinuxDeploymentV2.File> declarations, SegmentClassicLinuxDeploymentV2.Loading loading,
            long deadline) throws IOException {
        requireTime(deadline);
        var members = List.copyOf(declarations);
        if (members.size() > 1024) {
            throw new IOException("Candidate context file table exceeds its original bound");
        }
        try {
            SegmentClassicLinuxDeploymentV2.requireInterpreterReference(members, loading);
            SegmentClassicLinuxDeploymentV2.requireContextReferences(members, loading);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Invalid candidate context subgraph or fixed interpreter reference", invalid);
        }
        requireTime(deadline);
        return members;
    }

    private Map<Path, String> linuxContextAliases(SegmentClassicLinuxDeploymentV2.Loading loading, long deadline)
            throws IOException {
        Map<Path, String> aliasDeclarations = new HashMap<>();
        for (var alias : loading.aliases()) {
            aliasDeclarations.put(linuxMaterialPath(alias.link(), deadline), alias.target());
        }
        return fixedLoadingAliases(aliasDeclarations, deadline);
    }

    /** Only the original rule index/deadline; no caller name, Path, candidate or previous observation. */
    synchronized LinuxNeededDirectObservation verifyLinuxNeededDirect(int ruleIndex, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            return observeLinuxNeededDirect(deployment.files(), deployment.loading(), ruleIndex, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Independent original-table data only; actual name-reuse and audit history remain unobserved. */
    synchronized LinuxNeededDirectObservation readLinuxNeededDirect(List<SegmentClassicLinuxDeploymentV2.File> members,
            SegmentClassicLinuxDeploymentV2.Loading loading, int ruleIndex, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxNeededDirect(members, loading, ruleIndex, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private LinuxNeededDirectObservation observeLinuxNeededDirect(List<SegmentClassicLinuxDeploymentV2.File> declared,
            SegmentClassicLinuxDeploymentV2.Loading loading, int ruleIndex, long deadline) throws IOException {
        var members = fixedLinuxContextMembers(declared, loading, deadline);
        if (ruleIndex < 0 || ruleIndex >= loading.needed().size()) {
            throw new IOException("Direct NEEDED index is outside the original rule table");
        }
        var rule = loading.needed().get(ruleIndex);
        var context = loading.contexts().get(rule.requester());
        var metadata = linuxElfDeclaration(loading, context.file());
        String name = metadata.needed().get(rule.ordinal());
        boolean knownToken = SegmentClassicLinuxTokens.hasKnownToken(name, deadline);
        if (name.isEmpty() || name.charAt(0) != '/' || knownToken) {
            throw new IOException("Unsupported NEEDED name for a literal absolute direct-path observation");
        }
        var aliases = linuxContextAliases(loading, deadline);
        var requester = observeLinuxLoadPath(rule.requester(), context, members.get(context.file()), metadata,
                loading.interpreter(), aliases, deadline);
        if (!name.equals(requester.content().elf().needed().get(rule.ordinal()))) {
            throw new IOException("Original direct NEEDED name differs from its requester ELF observation");
        }
        Path entry = loadingNativePath(name, deadline);
        var probe = observeLinuxPath(LinuxPathKind.LOOKUP, entry, aliases, false, deadline);
        var candidate = observeLinuxNeededCandidate(rule, name, probe, members, loading, aliases, deadline);
        if (!probe.equals(observeLinuxPath(LinuxPathKind.LOOKUP, entry, aliases, false, deadline))) {
            throw new IOException("NEEDED direct path changed across its observation");
        }
        var result = new LinuxNeededDirectObservation(ruleIndex, rule, name, requester, probe, candidate);
        files.requireHealthy();
        requireTime(deadline);
        return result;
    }

    /** Only original rule/directory indices; no caller name, Path, candidate or previous observation. */
    synchronized LinuxNeededLookupObservation verifyLinuxNeededLookup(int ruleIndex, int directoryIndex, long deadline)
            throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            return observeLinuxNeededLookup(deployment.files(), deployment.loading(), ruleIndex, directoryIndex,
                    deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Independent original-table data only, never provider/source readiness or a complete deployment proof. */
    synchronized LinuxNeededLookupObservation readLinuxNeededLookup(List<SegmentClassicLinuxDeploymentV2.File> members,
            SegmentClassicLinuxDeploymentV2.Loading loading, int ruleIndex, int directoryIndex, long deadline)
            throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxNeededLookup(members, loading, ruleIndex, directoryIndex, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private LinuxNeededLookupObservation observeLinuxNeededLookup(List<SegmentClassicLinuxDeploymentV2.File> declared,
            SegmentClassicLinuxDeploymentV2.Loading loading, int ruleIndex, int directoryIndex, long deadline)
            throws IOException {
        var members = fixedLinuxContextMembers(declared, loading, deadline);
        if (ruleIndex < 0 || ruleIndex >= loading.needed().size()
                || directoryIndex < 0 || directoryIndex >= loading.directories().size()) {
            throw new IOException("NEEDED lookup index is outside the original rule or directory table");
        }
        var rule = loading.needed().get(ruleIndex);
        var context = loading.contexts().get(rule.requester());
        var metadata = linuxElfDeclaration(loading, context.file());
        String name = metadata.needed().get(rule.ordinal());
        if (name.isEmpty() || name.indexOf('/') >= 0 || name.indexOf('$') >= 0
                || name.equals(".") || name.equals("..")) {
            throw new IOException("Unsupported NEEDED name for an ordinary base-directory lookup");
        }
        var declaredDirectory = loading.directories().get(directoryIndex);
        String dirname = SegmentClassicLinuxDynamicPath.dirname(declaredDirectory.path(), deadline);
        if (name.getBytes(StandardCharsets.UTF_8).length > 4096 - dirname.getBytes(StandardCharsets.UTF_8).length) {
            throw new IOException("NEEDED candidate path exceeds its fixed UTF8 byte bound");
        }
        String logical = dirname + name;
        var aliases = linuxContextAliases(loading, deadline);
        var requester = observeLinuxLoadPath(rule.requester(), context, members.get(context.file()), metadata,
                loading.interpreter(), aliases, deadline);
        if (!name.equals(requester.content().elf().needed().get(rule.ordinal()))) {
            throw new IOException("Original NEEDED name differs from the requester ELF observation");
        }
        var directory = new LinuxSearchDirectory(directoryIndex, declaredDirectory.path(), declaredDirectory.present());
        Path directoryEntry = loadingNativePath(directory.logical(), deadline);
        var beforeDirectory = searchDirectoryPath(directory, directoryEntry, aliases, LinuxPathKind.LOOKUP, deadline);
        Path entry = loadingNativePath(logical, deadline);
        var probe = observeLinuxPath(LinuxPathKind.LOOKUP, entry, aliases, false, deadline);
        var candidate = observeLinuxNeededCandidate(rule, logical, probe, members, loading, aliases, deadline);
        if (!probe.equals(observeLinuxPath(LinuxPathKind.LOOKUP, entry, aliases, false, deadline))
                || !beforeDirectory.equals(searchDirectoryPath(directory, directoryEntry, aliases, LinuxPathKind.LOOKUP,
                        deadline))) {
            throw new IOException("NEEDED candidate or declared directory changed across its lookup observation");
        }
        var result = new LinuxNeededLookupObservation(ruleIndex, directoryIndex, rule, logical, beforeDirectory, probe,
                requester, candidate);
        files.requireHealthy();
        requireTime(deadline);
        return result;
    }

    /** Only original indices and deadline; this is the fixed unfiltered rule-one set, never actual capstr. */
    synchronized LinuxNeededDirectoryObservation verifyLinuxNeededDirectoryPositions(int ruleIndex,
            int directoryIndex, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            return observeLinuxNeededDirectoryPositions(deployment.files(), deployment.loading(), ruleIndex,
                    directoryIndex, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Independent declaration/file fixture only; does not create source, Installed or capability evidence. */
    synchronized LinuxNeededDirectoryObservation readLinuxNeededDirectoryPositions(
            List<SegmentClassicLinuxDeploymentV2.File> members, SegmentClassicLinuxDeploymentV2.Loading loading,
            int ruleIndex, int directoryIndex, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxNeededDirectoryPositions(members, loading, ruleIndex, directoryIndex, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private LinuxNeededDirectoryObservation observeLinuxNeededDirectoryPositions(
            List<SegmentClassicLinuxDeploymentV2.File> declared, SegmentClassicLinuxDeploymentV2.Loading loading,
            int ruleIndex, int directoryIndex, long deadline) throws IOException {
        var members = fixedLinuxContextMembers(declared, loading, deadline);
        if (loading.policy().hwcaps() != 1 || ruleIndex < 0 || ruleIndex >= loading.needed().size()
                || directoryIndex < 0 || directoryIndex >= loading.directories().size()) {
            throw new IOException("Unsupported built-in hwcap rule or original NEEDED/directory index");
        }
        var rule = loading.needed().get(ruleIndex);
        var context = loading.contexts().get(rule.requester());
        var metadata = linuxElfDeclaration(loading, context.file());
        String name = metadata.needed().get(rule.ordinal());
        if (name.isEmpty() || name.indexOf('/') >= 0 || name.indexOf('$') >= 0
                || name.equals(".") || name.equals("..")) {
            throw new IOException("Unsupported NEEDED name for fixed built-in directory positions");
        }
        var logicals = builtinLogicalPaths(loading, directoryIndex, name, deadline);
        var aliases = linuxContextAliases(loading, deadline);
        var requester = observeLinuxLoadPath(rule.requester(), context, members.get(context.file()), metadata,
                loading.interpreter(), aliases, deadline);
        if (!name.equals(requester.content().elf().needed().get(rule.ordinal()))) {
            throw new IOException("Original built-in NEEDED differs from its requester ELF observation");
        }
        return observeLinuxDirectoryPositions(ruleIndex, directoryIndex, requester, logicals, members, loading,
                aliases, deadline);
    }

    private static List<String> builtinLogicalPaths(SegmentClassicLinuxDeploymentV2.Loading loading,
            int directoryIndex, String name, long deadline) throws IOException {
        var declaredDirectory = loading.directories().get(directoryIndex);
        String dirname = SegmentClassicLinuxDynamicPath.dirname(declaredDirectory.path(), deadline);
        int bytes = dirname.getBytes(StandardCharsets.UTF_8).length + name.getBytes(StandardCharsets.UTF_8).length;
        var logicals = new ArrayList<String>(BUILTIN_HWCAP_SUFFIXES.size());
        for (String suffix : BUILTIN_HWCAP_SUFFIXES) {
            requireTime(deadline);
            if (bytes > 4096 - suffix.length()) {
                throw new IOException("Fixed built-in candidate path exceeds its UTF8 byte bound");
            }
            String logical = dirname + suffix + name;
            SegmentClassicLinuxGlob.requireLogicalText(logical);
            logicals.add(logical);
        }
        return List.copyOf(logicals);
    }

    private LinuxNeededDirectoryObservation observeLinuxDirectoryPositions(int ruleIndex, int directoryIndex,
            LinuxLoadPathObservation requester, List<String> logicals,
            List<SegmentClassicLinuxDeploymentV2.File> members, SegmentClassicLinuxDeploymentV2.Loading loading,
            Map<Path, String> aliases, long deadline) throws IOException {
        var rule = loading.needed().get(ruleIndex);
        String name = requester.content().elf().needed().get(rule.ordinal());
        var declaredDirectory = loading.directories().get(directoryIndex);
        var directory = new LinuxSearchDirectory(directoryIndex, declaredDirectory.path(), declaredDirectory.present());
        Path directoryEntry = loadingNativePath(directory.logical(), deadline);
        var beforeDirectory = searchDirectoryPath(directory, directoryEntry, aliases, LinuxPathKind.LOOKUP, deadline);
        var positions = new ArrayList<LinuxNeededDirectoryPosition>(BUILTIN_HWCAP_SUFFIXES.size());
        for (int i = 0; i < logicals.size(); i++) {
            String logical = logicals.get(i);
            Path entry = loadingNativePath(logical, deadline);
            var probe = observeLinuxPath(LinuxPathKind.LOOKUP, entry, aliases, false, deadline);
            var candidate = observeLinuxNeededCandidate(rule, logical, probe, members, loading, aliases, deadline);
            positions.add(new LinuxNeededDirectoryPosition(i, BUILTIN_HWCAP_SUFFIXES.get(i), logical, probe,
                    candidate));
        }
        var result = new LinuxNeededDirectoryObservation(ruleIndex, directoryIndex, rule, name, beforeDirectory,
                requester, positions);
        recheckLinuxDirectoryPositions(result, loading, aliases, deadline);
        files.requireHealthy();
        requireTime(deadline);
        return result;
    }

    private void recheckLinuxDirectoryPositions(LinuxNeededDirectoryObservation observed,
            SegmentClassicLinuxDeploymentV2.Loading loading, Map<Path, String> aliases, long deadline)
            throws IOException {
        recheckLinuxDirectoryPaths(directoryPaths(observed), loading, aliases, deadline);
    }

    private static LinuxDirectoryPaths directoryPaths(LinuxNeededDirectoryObservation observed) {
        return new LinuxDirectoryPaths(observed.directory(), observed.directoryPath(),
                observed.positions().stream().map(LinuxNeededDirectoryPosition::path).toList());
    }

    private void recheckLinuxDirectoryPaths(LinuxDirectoryPaths observed,
            SegmentClassicLinuxDeploymentV2.Loading loading, Map<Path, String> aliases, long deadline)
            throws IOException {
        var declaredDirectory = loading.directories().get(observed.directory());
        var directory = new LinuxSearchDirectory(observed.directory(), declaredDirectory.path(),
                declaredDirectory.present());
        Path directoryEntry = loadingNativePath(directory.logical(), deadline);
        for (var position : observed.positions()) {
            var terminal = observeLinuxPath(LinuxPathKind.LOOKUP, position.entry(), aliases, false, deadline);
            if (!position.equals(terminal)) {
                throw new IOException("Fixed built-in candidate changed across its directory observation");
            }
        }
        if (!observed.directoryPath().equals(searchDirectoryPath(directory, directoryEntry, aliases,
                LinuxPathKind.LOOKUP, deadline))) {
            throw new IOException("Original base directory changed across its built-in position observation");
        }
    }

    /** Only the original rule/deadline; never caller directories, token values, chain or a directory plan. */
    synchronized LinuxNeededRequesterTagObservation verifyLinuxNeededRequesterTag(int ruleIndex, long deadline)
            throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            return observeLinuxNeededRequesterTag(deployment.files(), deployment.loading(), ruleIndex, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Original-table file fixture only, not source, stage reachability or runtime search qualification. */
    synchronized LinuxNeededRequesterTagObservation readLinuxNeededRequesterTag(
            List<SegmentClassicLinuxDeploymentV2.File> members, SegmentClassicLinuxDeploymentV2.Loading loading,
            int ruleIndex, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxNeededRequesterTag(members, loading, ruleIndex, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private LinuxNeededRequesterTagObservation observeLinuxNeededRequesterTag(
            List<SegmentClassicLinuxDeploymentV2.File> declared, SegmentClassicLinuxDeploymentV2.Loading loading,
            int ruleIndex, long deadline) throws IOException {
        var members = fixedLinuxContextMembers(declared, loading, deadline);
        var plan = planLinuxNeededRequesterTag(loading, ruleIndex, deadline);
        // Complete all tag/mapping/joined-path preflight before even the requester is opened.
        var aliases = linuxContextAliases(loading, deadline);
        var requester = observeLinuxLoadPath(plan.rule().requester(), plan.context(),
                members.get(plan.context().file()), plan.metadata(), loading.interpreter(), aliases, deadline);
        var directories = new ArrayList<LinuxNeededDirectoryObservation>();
        observeLinuxRequesterTagDirectories(members, loading, ruleIndex, requester, plan, aliases, directories,
                deadline);
        return new LinuxNeededRequesterTagObservation(ruleIndex, plan.rule(), plan.name(), plan.tag(),
                plan.declaration(), requester, directories);
    }

    private static LinuxNeededRequesterTagPlan planLinuxNeededRequesterTag(
            SegmentClassicLinuxDeploymentV2.Loading loading, int ruleIndex, long deadline) throws IOException {
        requireTime(deadline);
        if (loading.policy().hwcaps() != 1 || ruleIndex < 0 || ruleIndex >= loading.needed().size()) {
            throw new IOException("Unsupported requester-tag policy or original NEEDED index");
        }
        var rule = loading.needed().get(ruleIndex);
        var context = loading.contexts().get(rule.requester());
        var metadata = linuxElfDeclaration(loading, context.file());
        String name = metadata.needed().get(rule.ordinal());
        if (name.isEmpty() || name.indexOf('/') >= 0 || name.indexOf('$') >= 0
                || name.equals(".") || name.equals("..")) {
            throw new IOException("Unsupported NEEDED name for this requester's directory tag");
        }
        LinuxRequesterTag tag = metadata.runpath().present() ? LinuxRequesterTag.RUNPATH
                : metadata.rpath().present() ? LinuxRequesterTag.RPATH : LinuxRequesterTag.NONE;
        var declaration = tag == LinuxRequesterTag.RUNPATH ? metadata.runpath()
                : tag == LinuxRequesterTag.RPATH ? metadata.rpath() : null;
        var plans = new ArrayList<LinuxDirectoryPlan>();
        if (declaration != null) {
            var unknown = SegmentClassicLinuxTokens.Value.unobserved();
            var expanded = SegmentClassicLinuxDynamicPath.expand(declaration, unknown, unknown, unknown, deadline);
            if (expanded.state() != SegmentClassicLinuxTokens.State.AVAILABLE) {
                throw new IOException("Unsupported requester tag depends on unobserved token sources");
            }
            for (var directory : expanded.directories()) {
                requireTime(deadline);
                if (directory.kind() != SegmentClassicLinuxDynamicPath.Kind.DIRECTORY
                        || !directory.dirname().startsWith("/")) {
                    throw new IOException("Unsupported requester tag contains CWD or a relative directory");
                }
                int index = requesterTagDirectory(loading, directory.dirname(), deadline);
                plans.add(new LinuxDirectoryPlan(index, builtinLogicalPaths(loading, index, name, deadline)));
            }
        }
        return new LinuxNeededRequesterTagPlan(rule, context, metadata, name, tag, declaration, plans);
    }

    private void observeLinuxRequesterTagDirectories(
            List<SegmentClassicLinuxDeploymentV2.File> members, SegmentClassicLinuxDeploymentV2.Loading loading,
            int ruleIndex, LinuxLoadPathObservation requester, LinuxNeededRequesterTagPlan plan,
            Map<Path, String> aliases, List<LinuxNeededDirectoryObservation> details, long deadline)
            throws IOException {
        var actual = requester.content().elf();
        if (requester.context() != plan.rule().requester() || requester.member() != plan.context().file()
                || !requester.logical().equals(plan.context().logicalLoadName())
                || !actual.rpath().equals(plan.metadata().rpath())
                || !actual.runpath().equals(plan.metadata().runpath())
                || !plan.name().equals(actual.needed().get(plan.rule().ordinal()))) {
            throw new IOException("Original tag NEEDED differs from its requester ELF observation");
        }
        observeLinuxPlannedDirectories(members, loading, ruleIndex, requester, plan.directories(), aliases,
                details, deadline);
        var terminal = observeLinuxPath(LinuxPathKind.LOOKUP, loadingNativePath(requester.logical(), deadline),
                aliases, true, deadline);
        if (!requester.path().equals(terminal) || !requester.content().identity().equals(terminal.identity())) {
            throw new IOException("Original requester path changed during its tag observations");
        }
        files.requireHealthy();
        requireTime(deadline);
    }

    private static int requesterTagDirectory(SegmentClassicLinuxDeploymentV2.Loading loading, String dirname,
                                              long deadline) throws IOException {
        int found = -1;
        for (int i = 0; i < loading.directories().size(); i++) {
            requireTime(deadline);
            String raw = loading.directories().get(i).path();
            // Such an unrelated declaration cannot equal any supported <=4096-byte expanded dirname.
            if (SegmentClassicLinuxTokens.utf8Length(raw, deadline) == 4096 && !raw.endsWith("/")) {
                continue;
            }
            if (dirname.equals(SegmentClassicLinuxDynamicPath.dirname(raw, deadline))) {
                if (found != -1) {
                    throw new IOException("Requester tag dirname has ambiguous original directory declarations");
                }
                found = i;
            }
        }
        if (found == -1) {
            throw new IOException("Requester tag dirname lacks an original directory declaration");
        }
        return found;
    }

    /** Only an original rule and deadline; policy order and requester flags come from this original owner. */
    synchronized LinuxNeededDefaultObservation verifyLinuxNeededDefaults(int ruleIndex, long deadline)
            throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            return observeLinuxNeededDefaults(deployment.files(), deployment.loading(), ruleIndex, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Independent pure declarations and actual file fixture, not typed source, Installed or runtime search. */
    synchronized LinuxNeededDefaultObservation readLinuxNeededDefaults(
            List<SegmentClassicLinuxDeploymentV2.File> members,
            SegmentClassicLinuxDeploymentV2.Loading loading, int ruleIndex, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxNeededDefaults(members, loading, ruleIndex, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private LinuxNeededDefaultObservation observeLinuxNeededDefaults(
            List<SegmentClassicLinuxDeploymentV2.File> declared,
            SegmentClassicLinuxDeploymentV2.Loading loading, int ruleIndex, long deadline) throws IOException {
        var members = fixedLinuxContextMembers(declared, loading, deadline);
        var plan = planLinuxNeededDefaults(loading, ruleIndex, deadline);
        // Complete the original preflight before even the standalone requester is opened.
        var aliases = linuxContextAliases(loading, deadline);
        var requester = observeLinuxLoadPath(plan.rule().requester(), plan.context(),
                members.get(plan.context().file()), plan.metadata(), loading.interpreter(), aliases, deadline);
        var observations = new ArrayList<LinuxNeededDirectoryObservation>();
        var state = observeLinuxDefaultDirectories(members, loading, ruleIndex, requester, plan, aliases, observations,
                deadline);
        var directories = new ArrayList<LinuxDefaultDirectoryObservation>();
        for (var observed : observations) {
            directories.add(new LinuxDefaultDirectoryObservation(directories.size(), observed));
        }
        var actual = requester.content().elf();
        return new LinuxNeededDefaultObservation(ruleIndex, plan.rule(), plan.name(), requester, actual.flags1Present(),
                actual.flags1(), state, directories);
    }

    private static LinuxNeededDefaultPlan planLinuxNeededDefaults(SegmentClassicLinuxDeploymentV2.Loading loading,
            int ruleIndex, long deadline) throws IOException {
        requireTime(deadline);
        if (loading.policy().hwcaps() != 1 || ruleIndex < 0 || ruleIndex >= loading.needed().size()) {
            throw new IOException("Unsupported default-stage policy or original NEEDED index");
        }
        final List<Integer> indices;
        try {
            indices = SegmentClassicLinuxDeploymentV2.defaultDirectoryIndices(loading);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("Default-stage directory references differ from their original declarations",
                    invalid);
        }
        var rule = loading.needed().get(ruleIndex);
        var context = loading.contexts().get(rule.requester());
        var metadata = linuxElfDeclaration(loading, context.file());
        String name = metadata.needed().get(rule.ordinal());
        if (name.isEmpty() || name.indexOf('/') >= 0 || name.indexOf('$') >= 0
                || name.equals(".") || name.equals("..")) {
            throw new IOException("Unsupported NEEDED name for an ordinary default-directory stage");
        }
        var plans = new ArrayList<LinuxDirectoryPlan>();
        if ((metadata.flags1() & DF_1_NODEFLIB) == 0) {
            for (int directory : indices) {
                plans.add(new LinuxDirectoryPlan(directory, builtinLogicalPaths(loading, directory, name, deadline)));
            }
        }
        return new LinuxNeededDefaultPlan(rule, context, metadata, name, plans);
    }

    private LinuxDefaultState observeLinuxDefaultDirectories(
            List<SegmentClassicLinuxDeploymentV2.File> members, SegmentClassicLinuxDeploymentV2.Loading loading,
            int ruleIndex, LinuxLoadPathObservation requester, LinuxNeededDefaultPlan plan, Map<Path, String> aliases,
            List<LinuxNeededDirectoryObservation> details, long deadline) throws IOException {
        var actual = requester.content().elf();
        if (requester.context() != plan.rule().requester() || requester.member() != plan.context().file()
                || !requester.logical().equals(plan.context().logicalLoadName())
                || actual.flags1() != plan.metadata().flags1()
                || !plan.name().equals(actual.needed().get(plan.rule().ordinal()))) {
            throw new IOException("Original default-stage NEEDED differs from its requester ELF observation");
        }
        if ((actual.flags1() & DF_1_NODEFLIB) != 0) {
            files.requireHealthy();
            requireTime(deadline);
            return LinuxDefaultState.SUPPRESSED_NODEFLIB;
        }
        observeLinuxPlannedDirectories(members, loading, ruleIndex, requester, plan.directories(), aliases,
                details, deadline);
        files.requireHealthy();
        requireTime(deadline);
        return LinuxDefaultState.BUILTIN_POSITIONS_OBSERVED;
    }

    private void observeLinuxPlannedDirectories(
            List<SegmentClassicLinuxDeploymentV2.File> members, SegmentClassicLinuxDeploymentV2.Loading loading,
            int ruleIndex, LinuxLoadPathObservation requester, List<LinuxDirectoryPlan> plans,
            Map<Path, String> aliases, List<LinuxNeededDirectoryObservation> details, long deadline)
            throws IOException {
        var paths = new ArrayList<LinuxDirectoryPaths>();
        for (var directory : plans) {
            var observed = observeLinuxDirectoryPositions(ruleIndex, directory.directory(), requester,
                    directory.logicals(), members, loading, aliases, deadline);
            paths.add(directoryPaths(observed));
            if (details != null) {
                details.add(observed);
            }
        }
        for (var directory : paths) {
            recheckLinuxDirectoryPaths(directory, loading, aliases, deadline);
        }
    }

    /** Only an original NEEDED rule and deadline; the fixed cache configuration is selected from this owner. */
    synchronized LinuxNeededCacheObservation verifyLinuxNeededCache(int ruleIndex, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            int cacheRule = fixedLinuxCacheRule(deployment.loading());
            var members = fixedLinuxContextMembers(deployment.files(), deployment.loading(), deadline);
            return observeLinuxNeededCache(members, deployment.loading(), ruleIndex, cacheRule, null, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** Independent declaration/file fixture only; it cannot establish typed source, Installed or a loader choice. */
    synchronized LinuxNeededCacheObservation readLinuxNeededCache(List<SegmentClassicLinuxDeploymentV2.File> members,
            SegmentClassicLinuxDeploymentV2.Loading loading, int ruleIndex, int cacheRule, long deadline)
            throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxNeededCache(fixedLinuxContextMembers(members, loading, deadline), loading, ruleIndex,
                    cacheRule, null, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    static int fixedLinuxCacheRule(SegmentClassicLinuxDeploymentV2.Loading loading) throws IOException {
        int selected = -1;
        for (int i = 0; i < loading.configuration().size(); i++) {
            var rule = loading.configuration().get(i);
            if (rule.kind() == ConfigurationKind.CACHE && rule.path().equals("/etc/ld.so.cache")) {
                if (selected != -1) {
                    throw new IOException("Conflicting original fixed loader cache rules");
                }
                selected = i;
            }
        }
        if (selected == -1) {
            throw new IOException("Unsupported lookup without the original fixed /etc/ld.so.cache CACHE rule");
        }
        return selected;
    }

    private LinuxNeededCacheObservation observeLinuxNeededCache(List<SegmentClassicLinuxDeploymentV2.File> members,
            SegmentClassicLinuxDeploymentV2.Loading loading, int ruleIndex, int cacheRule,
            LinuxLoadPathObservation boundRequester, long deadline) throws IOException {
        requireTime(deadline);
        if (ruleIndex < 0 || ruleIndex >= loading.needed().size()) {
            throw new IOException("Cache NEEDED index is outside the original rule table");
        }
        var configuration = selectLinuxConfiguration(members, loading, cacheRule);
        if (configuration.kind() != ConfigurationKind.CACHE) {
            throw new IOException("NEEDED cache lookup requires its original CACHE configuration rule");
        }
        var rule = loading.needed().get(ruleIndex);
        var context = loading.contexts().get(rule.requester());
        var metadata = linuxElfDeclaration(loading, context.file());
        String name = metadata.needed().get(rule.ordinal());
        if (name.isEmpty() || name.indexOf('/') >= 0 || name.indexOf('$') >= 0
                || name.equals(".") || name.equals("..")) {
            throw new IOException("Unsupported NEEDED name for an ordinary cache lookup");
        }
        SegmentClassicLinuxCache.validateLookupName(name, deadline);
        var aliases = linuxContextAliases(loading, deadline);
        var requester = observeLinuxLoadPath(rule.requester(), context, members.get(context.file()), metadata,
                loading.interpreter(), aliases, deadline);
        if (boundRequester != null && !boundRequester.equals(requester)) {
            throw new IOException("JDK helper requester changed before its original NEEDED cache lookup");
        }
        if (!name.equals(requester.content().elf().needed().get(rule.ordinal()))) {
            throw new IOException("Original cache NEEDED differs from its requester ELF observation");
        }
        Path cachePath = linuxMaterialPath(configuration.path(), deadline);
        var before = observeLoaderConfiguration(members, cacheRule, configuration, cachePath, aliases, deadline);
        var rows = new ArrayList<LinuxNeededCacheRow>();
        var paths = new HashMap<SegmentClassicLinuxCache.Text, LinuxNeededCacheRow>();
        var budget = new LoadingDirectoryBudget();
        if (before.material() != null) {
            for (var entry : SegmentClassicLinuxCache.related(before.material().cache(), name, deadline)) {
                requireTime(deadline);
                var fact = paths.get(entry.path());
                if (fact == null) {
                    budget.retain(entry.path().byteLength());
                    // Exact raw values share only this pass's fact; no expansion, splitting or path normalization.
                    String logical = entry.path().utf8();
                    Path path = loadingNativePath(logical, deadline);
                    var probe = observeLinuxPath(LinuxPathKind.LOOKUP, path, aliases, false, deadline);
                    var candidate = observeLinuxNeededCandidate(rule, logical, probe, members, loading, aliases,
                            deadline);
                    fact = new LinuxNeededCacheRow(entry, logical, probe, candidate);
                    paths.put(entry.path(), fact);
                }
                rows.add(new LinuxNeededCacheRow(entry, fact.logical(), fact.path(), fact.candidate()));
            }
        }
        var after = observeLoaderConfiguration(members, cacheRule, configuration, cachePath, aliases, deadline);
        requireLinuxCacheBinding(before, after);
        for (var row : paths.values()) {
            var terminal = observeLinuxPath(LinuxPathKind.LOOKUP, loadingNativePath(row.logical(), deadline), aliases,
                    false, deadline);
            if (!row.path().equals(terminal)) {
                throw new IOException("Cache candidate changed across its related-row observation");
            }
        }
        if (!after.path().equals(observeLinuxPath(LinuxPathKind.CONFIGURATION, cachePath, aliases,
                configuration.file() >= 0, deadline))) {
            throw new IOException("Original cache path or material identity changed during candidate rechecks");
        }
        files.requireHealthy();
        requireTime(deadline);
        return new LinuxNeededCacheObservation(ruleIndex, rule, name, requester, after, rows);
    }

    private static void requireLinuxCacheBinding(LinuxLoaderConfigurationObservation before,
            LinuxLoaderConfigurationObservation after) throws IOException {
        if (!before.path().equals(after.path()) || !Objects.equals(before.sha256(), after.sha256())
                || (before.material() == null) != (after.material() == null)
                || before.material() != null && (!before.material().identity().equals(after.material().identity())
                    || !before.material().parents().equals(after.material().parents()))) {
            throw new IOException("Original cache path, owned identity or hashed material changed across lookup");
        }
    }

    private LinuxLoadPathObservation observeLinuxNeededCandidate(SegmentClassicLinuxDeploymentV2.NeededRule rule,
            String logical, LinuxPathObservation probe, List<SegmentClassicLinuxDeploymentV2.File> members,
            SegmentClassicLinuxDeploymentV2.Loading loading, Map<Path, String> aliases, long deadline)
            throws IOException {
        LinuxLoadPathObservation candidate = null;
        if (probe.identity() != null) {
            if (probe.identity().directory()) {
                throw new IOException("NEEDED candidate is a directory, not an approved ELF member");
            }
            int approved = -1;
            for (int index : rule.candidates()) {
                requireTime(deadline);
                var target = loading.contexts().get(index);
                if (target.logicalLoadName().equals(logical)
                        && probe.resolved().equals(linuxMaterialPath(members.get(target.file()).path(), deadline))) {
                    if (approved != -1) {
                        throw new IOException("NEEDED lookup has ambiguous approved logical contexts");
                    }
                    approved = index;
                }
            }
            if (approved == -1) {
                throw new IOException("Present NEEDED candidate lacks this rule's exact approved logical context");
            }
            var target = loading.contexts().get(approved);
            candidate = observeLinuxLoadPath(approved, target, members.get(target.file()),
                    linuxElfDeclaration(loading, target.file()), loading.interpreter(), aliases, deadline);
            if (!probe.equals(candidate.path()) || !probe.identity().equals(candidate.content().identity())) {
                throw new IOException("NEEDED candidate changed between its lookup probe and owned ELF observation");
            }
        }
        return candidate;
    }

    private SegmentClassicLinuxDeploymentV2 originalLinuxDeployment(long deadline) throws IOException {
        requireBoundDeadline(deadline);
        requireTime(deadline);
        if (!production || installed == null || verifiedProgram == null || verifiedRuntimeImage == null
                || launcherSource == null || launcherSourceIdentity == null || launcherInput == null
                || launcherInput.linuxDeployment() == null || launcherInput.linuxDeployment().schema() != 2
                || linuxProvider == null || !sameProvider(linuxProvider, providerAtDecode)) {
            throw new IOException("D2 member requires this owner's original provider/source binding");
        }
        if (!sameProvider(linuxProvider, linuxProviderObjects())) {
            throw new IOException("Default implementation changed before original D2 member I/O");
        }
        return launcherInput.linuxDeployment().versionTwo();
    }

    private Path linuxPhysicalPath(String original, long deadline) throws IOException {
        Path path = linuxProvider.filesystem().getPath(original);
        requirePhysicalBytes(original, path, linuxProvider, deadline);
        return path;
    }

    private Map<Path, String> originalLinuxAliases(SegmentClassicLinuxDeploymentV2 deployment, long deadline)
            throws IOException {
        Map<Path, String> declarations = new HashMap<>();
        for (var alias : deployment.loading().aliases()) {
            declarations.put(linuxPhysicalPath(alias.link(), deadline), alias.target());
        }
        return fixedLoadingAliases(declarations, deadline);
    }

    /** Only an original configuration rule index; no caller material, graph, kind or source permit. */
    synchronized LinuxLoaderConfigurationObservation verifyLinuxLoaderConfiguration(int ruleIndex, long deadline)
            throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            var deployment = originalLinuxDeployment(deadline);
            var rule = selectLinuxConfiguration(deployment, ruleIndex);
            LinuxLoaderConfigurationObservation result;
            if (rule.kind() == ConfigurationKind.LDCONFIG) {
                var graph = observeLinuxConfigurationGraph(deployment.files(), deployment.loading(), ruleIndex,
                        deadline);
                result = new LinuxLoaderConfigurationObservation(ruleIndex, rule.kind(), null, null, null, graph);
            } else {
                var aliases = originalLinuxAliases(deployment, deadline);
                Path path = linuxPhysicalPath(rule.path(), deadline);
                var first = observeLoaderConfiguration(deployment.files(), ruleIndex, rule, path, aliases, deadline);
                var second = observeLoaderConfiguration(deployment.files(), ruleIndex, rule, path, aliases, deadline);
                if (rule.kind() == ConfigurationKind.CACHE) {
                    requireLinuxCacheBinding(first, second);
                } else if (!first.equals(second)) {
                    throw new IOException("Original loader cache or preload path changed between observations");
                }
                result = first;
            }
            files.requireHealthy();
            requireTime(deadline);
            return result;
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    /** D2 graph data only; cannot establish this owner's original source/provider, Installed or qualification. */
    synchronized LinuxConfigurationGraph readLinuxDeploymentConfigurationGraph(
            List<SegmentClassicLinuxDeploymentV2.File> members, SegmentClassicLinuxDeploymentV2.Loading loading,
            int ruleIndex, long deadline) throws IOException {
        requireReady();
        initializingThread = Thread.currentThread();
        try {
            requireBoundDeadline(deadline);
            return observeLinuxConfigurationGraph(members, loading, ruleIndex, deadline);
        } catch (IOException | RuntimeException | Error failure) {
            launcherFailure = failure;
            throw failure;
        } finally {
            initializingThread = null;
        }
    }

    private LinuxConfigurationGraph observeLinuxConfigurationGraph(
            List<SegmentClassicLinuxDeploymentV2.File> declared, SegmentClassicLinuxDeploymentV2.Loading loading,
            int ruleIndex, long deadline) throws IOException {
        var members = List.copyOf(declared);
        if (members.size() > 1024) {
            throw new IOException("D2 configuration members exceed their original bounds");
        }
        var rule = selectLinuxConfiguration(members, loading, ruleIndex);
        if (rule.kind() != ConfigurationKind.LDCONFIG) {
            throw new IOException("D2 configuration graph requires its original LDCONFIG rule");
        }
        try {
            SegmentClassicLinuxDeploymentV2.requireConfigurationReferences(members, loading);
        } catch (IllegalArgumentException invalid) {
            throw new IOException("D2 configuration references differ from their original declarations", invalid);
        }
        requireTime(deadline);
        var aliases = linuxContextAliases(loading, deadline);
        var materials = new ArrayList<LinuxConfigurationMaterial>();
        for (var entry : loading.configuration()) {
            if (entry.file() >= 0) {
                var file = members.get(entry.file());
                materials.add(new LinuxConfigurationMaterial(entry.file(), linuxMaterialPath(file.path(), deadline),
                        file.length(), file.sha256(), entry.kind()));
            }
        }
        return configurationGraph(materials, aliases, linuxMaterialPath(rule.path(), deadline), rule.file(), true,
                deadline);
    }

    private LinuxLoaderConfigurationObservation observeLoaderConfiguration(
            List<SegmentClassicLinuxDeploymentV2.File> members,
            int ruleIndex, SegmentClassicLinuxDeploymentV2.Configuration rule, Path path, Map<Path, String> aliases,
            long deadline) throws IOException {
        var observed = observeLinuxPath(LinuxPathKind.CONFIGURATION, path, aliases, rule.file() >= 0, deadline);
        if (rule.file() == -1) {
            return new LinuxLoaderConfigurationObservation(ruleIndex, rule.kind(), observed, null, null, null);
        }
        var file = members.get(rule.file());
        Path physical = linuxMaterialPath(file.path(), deadline);
        if (!observed.resolved().equals(physical)) {
            throw new IOException("Original cache path differs from its declared physical material");
        }
        var material = hashFile(physical, file.length(), file.sha256(), false, LinuxContentKind.CACHE, true, deadline);
        if (!observed.identity().equals(material.identity())
                || !observed.equals(observeLinuxPath(LinuxPathKind.CONFIGURATION, path, aliases, true, deadline))) {
            throw new IOException("Original cache identity or logical path changed during its hash/parser read");
        }
        return new LinuxLoaderConfigurationObservation(ruleIndex, rule.kind(), observed, material.content().sha256(),
                material.legacy(), null);
    }

    /** Mandatory rule/kind/format dispatch only; no Path construction or authority. */
    static SegmentClassicLinuxDeploymentV2.Configuration selectLinuxConfiguration(
            SegmentClassicLinuxDeploymentV2 deployment, int ruleIndex) throws IOException {
        return selectLinuxConfiguration(deployment.files(), deployment.loading(), ruleIndex);
    }

    private static SegmentClassicLinuxDeploymentV2.Configuration selectLinuxConfiguration(
            List<SegmentClassicLinuxDeploymentV2.File> members, SegmentClassicLinuxDeploymentV2.Loading loading,
            int ruleIndex) throws IOException {
        var rules = loading.configuration();
        if (ruleIndex < 0 || ruleIndex >= rules.size()) {
            throw new IOException("D2 configuration index is outside the original rule table");
        }
        var rule = rules.get(ruleIndex);
        if (rule.format() != 1 || rule.kind() == ConfigurationKind.MODULE_RUNTIME) {
            throw new IOException("Unsupported independent configuration kind or format");
        }
        if (rule.kind() == ConfigurationKind.CACHE && rule.file() >= 0) {
            if (rule.file() >= members.size()
                    || members.get(rule.file()).role() != SegmentClassicLinuxDeployment.Role.LOADER_CACHE) {
                throw new IOException("Cache rule does not reference its original cache member");
            }
            long length = members.get(rule.file()).length();
            if (length < 48 || length > SegmentClassicLinuxCache.MAX_BYTES) {
                throw new IOException("Declared loader cache length exceeds its supported format bounds");
            }
        }
        return rule;
    }

    /** Pure original-table selection only. The caller still needs the owner's provider/source gate for I/O. */
    static SegmentClassicLinuxDeploymentV2.File selectLinuxElfMember(SegmentClassicLinuxDeploymentV2 deployment,
                                                                    int fileIndex) throws IOException {
        if (fileIndex < 0 || fileIndex >= deployment.files().size()) {
            throw new IOException("D2 ELF member index is outside the original file table");
        }
        var file = deployment.files().get(fileIndex);
        if (!file.role().elf()) {
            throw new IOException("D2 member does not have an approved ELF role");
        }
        return file;
    }

    /** Metadata comparison only, not a source, file hash or qualification proof. */
    static void requireLinuxElfMetadata(SegmentClassicLinuxDeployment.Role role, String interpreter,
            SegmentClassicLinuxDeploymentV2.Elf expected, SegmentClassicLinuxElf.Image2 observed) throws IOException {
        boolean executable = role.elf() && role != SegmentClassicLinuxDeployment.Role.LIBRARY
                && role != SegmentClassicLinuxDeployment.Role.MODULE;
        if (!role.elf() || executable && observed.entry() == 0 || !executable && observed.type() != 3) {
            throw new IOException("D2 ELF type or entry differs from its declared role");
        }
        if (!observed.needed().equals(expected.needed()) || !observed.rpath().equals(expected.rpath())
                || !observed.runpath().equals(expected.runpath()) || observed.flags() != expected.flags()
                || observed.flags1() != expected.flags1()
                || role == SegmentClassicLinuxDeployment.Role.SYSTEMCTL
                && !Objects.equals(observed.interpreter(), interpreter)) {
            throw new IOException("Original D2 ELF metadata or SYSTEMCTL interpreter differs");
        }
    }

    private LinuxFileObservation hashProgramFile(SegmentClassicLaunchProgram.File file, long deadlineNanos)
            throws IOException {
        return hashFile(file.path(), file.length(), file.sha256(),
                (file.roles() & SegmentClassicLaunchProgram.JAVA) != 0, LinuxContentKind.NONE, deadlineNanos).legacy();
    }

    private HashedFile hashFile(Path path, long declaredLength, SegmentClassicHandoff.Digest sha256,
            boolean executable, LinuxContentKind kind, long deadlineNanos)
            throws IOException {
        return hashFile(path, declaredLength, sha256, executable, kind, kind == LinuxContentKind.ELF2, deadlineNanos);
    }

    private HashedFile hashFile(Path path, long declaredLength, SegmentClassicHandoff.Digest sha256,
            boolean executable, LinuxContentKind kind, boolean bindIdentity, long deadlineNanos)
            throws IOException {
        requireTime(deadlineNanos);
        Identity before = checked(path);
        if (before.directory() || before.uid() != ownerUid || before.links() != 1
                || (before.mode() & 022) != 0 || before.size() != declaredLength
                || executable && (before.mode() & 0111) == 0) {
            throw new IOException("Program file identity, permissions or declared length mismatch");
        }
        var parents = chain(path);
        LinuxContent content;
        try (FileChannel channel = SegmentOwnedFiles.open(files, path,
                StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            requireTime(deadlineNanos);
            if (channel.size() != declaredLength) {
                throw new IOException("Program file length changed during open");
            }
            NativeIO.FileIdentity held = null;
            if (bindIdentity) {
                requireTime(deadlineNanos);
                held = files.fileIdentity(channel, nativeIo);
                requireTime(deadlineNanos);
                if (held.device() != before.device() || held.inode() != before.inode()) {
                    throw new IOException("Original " + (kind == LinuxContentKind.ELF2 ? "ELF2"
                            : kind == LinuxContentKind.LDCONFIG ? "configuration"
                            : kind == LinuxContentKind.CACHE ? "cache" : "file")
                            + " fd identity differs from its checked material path");
                }
            }
            content = readLinuxContent(channel, declaredLength, kind, deadlineNanos);
            if (bindIdentity) {
                requireTime(deadlineNanos);
                var after = files.fileIdentity(channel, nativeIo);
                requireTime(deadlineNanos);
                if (!held.equals(after)) {
                    throw new IOException("Original " + (kind == LinuxContentKind.ELF2 ? "ELF2"
                            : kind == LinuxContentKind.LDCONFIG ? "configuration"
                            : kind == LinuxContentKind.CACHE ? "cache" : "file")
                            + " fd identity changed during its hash/parser read");
                }
            }
            if (channel.size() != declaredLength || !before.equals(checked(path))) {
                throw new IOException("Program file changed during its stream hash");
            }
        }
        requireTime(deadlineNanos);
        if (!before.equals(checked(path)) || !parents.equals(chain(path))
                || !MessageDigest.isEqual(sha256.value(), content.sha256().value())) {
            throw new IOException("Program file, parent or SHA-256 differs from its fixed declaration");
        }
        files.requireHealthy();
        requireTime(deadlineNanos);
        return new HashedFile(before, parents, content);
    }

    /** One stream hash and fixed parser on the same held channel. Caller retains open/close and source checks. */
    static LinuxContent readLinuxContent(FileChannel channel, long length, LinuxContentKind kind, long deadline)
            throws IOException {
        requireTime(deadline);
        if (length < 0 || kind == null || kind == LinuxContentKind.CACHE
                && (length < 48 || length > SegmentClassicLinuxCache.MAX_BYTES)
                || kind == LinuxContentKind.LDCONFIG && length > SegmentClassicLinuxConfiguration.MAX_BYTES) {
            throw new IOException("Unsupported bounded Linux content kind or length");
        }
        if (kind == LinuxContentKind.CACHE) {
            var cache = SegmentClassicLinuxCache.readHashed(channel, length, deadline);
            channel.position(length);
            var result = new LinuxContent(cache.sha256(), null, null, cache.image(), null);
            requireTime(deadline);
            return result;
        }
        if (kind == LinuxContentKind.LDCONFIG) {
            var configuration = SegmentClassicLinuxConfiguration.readHashed(channel, length, deadline);
            channel.position(length);
            var result = new LinuxContent(configuration.sha256(), null, null, null, configuration.image());
            requireTime(deadline);
            return result;
        }
        if (kind == LinuxContentKind.ELF2) {
            var elf = SegmentClassicLinuxElf.readVersionTwoHashed(channel, length, deadline);
            var result = new LinuxContent(elf.sha256(), null, elf.image(), null, null);
            requireTime(deadline);
            return result;
        }
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (GeneralSecurityException unavailable) {
            throw new IOException("Required program SHA-256 is unavailable", unavailable);
        }
        if (channel.size() != length) {
            throw new IOException("Held Linux content channel length differs from its declaration");
        }
        channel.position(0);
        ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
        long remaining = length;
        while (remaining != 0) {
            requireTime(deadline);
            buffer.clear().limit((int) Math.min(buffer.capacity(), remaining));
            int read = channel.read(buffer);
            if (read <= 0) {
                throw new IOException("Program file read made no progress before the declared end");
            }
            remaining -= read;
            buffer.flip();
            digest.update(buffer);
        }
        SegmentClassicLinuxElf.Image elf = kind == LinuxContentKind.ELF1
                ? SegmentClassicLinuxElf.read(channel, length, deadline) : null;
        var result = new LinuxContent(new SegmentClassicHandoff.Digest(digest.digest()), elf, null, null,
                null);
        requireTime(deadline);
        return result;
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

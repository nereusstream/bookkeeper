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
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Bounded fixed parent input data only. Neither a signer, installed provenance nor process qualification. */
record SegmentClassicLauncherInputs(String recipeId, SegmentClassicHandoff.Digest recipe,
        SegmentClassicHandoff.Digest signerKeyId, SegmentClassicHandoff.Digest parentProgram, String os,
        String architecture, SegmentClassicLaunchProgram.File dropper, Path supervisorDirectory, long targetUid,
        Path manifest, Path proof, Path policy, Path aesKey, SegmentClassicLinuxDeployment linuxDeployment) {
    static final int MAX_BYTES = 32768;
    static final int MAX_V2_BYTES = 1024 * 1024;
    static final String MAIN = "org.apache.bookkeeper.server.ControlledClassicLauncherMain";
    private static final long MAGIC = 0x5557434c58303031L; // UWCLX001
    private static final long MAGIC_V2 = 0x5557434c58303032L; // UWCLX002
    private static final byte[] PARENT_DOMAIN = "BookKeeper/ControlledClassicLauncherProgram/v1\0"
            .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] PARENT_V2_DOMAIN = "BookKeeper/ControlledClassicLauncherProgram/v2\0"
            .getBytes(StandardCharsets.US_ASCII);

    SegmentClassicLauncherInputs(String recipeId, SegmentClassicHandoff.Digest recipe,
            SegmentClassicHandoff.Digest signerKeyId, SegmentClassicHandoff.Digest parentProgram, String os,
            String architecture, SegmentClassicLaunchProgram.File dropper, Path supervisorDirectory, long targetUid,
            Path manifest, Path proof, Path policy, Path aesKey) {
        this(recipeId, recipe, signerKeyId, parentProgram, os, architecture, dropper, supervisorDirectory, targetUid,
                manifest, proof, policy, aesKey, null);
    }

    SegmentClassicLauncherInputs {
        SegmentClassicLaunchCodec.recipeId(recipeId);
        Objects.requireNonNull(recipe, "recipe");
        Objects.requireNonNull(signerKeyId, "signerKeyId");
        Objects.requireNonNull(parentProgram, "parentProgram");
        if (!Set.of("linux", "macos").contains(os) || !Set.of("x86_64", "aarch64").contains(architecture)
                || dropper.roles() != SegmentClassicLaunchProgram.NATIVE
                || dropper.length() < 32 || dropper.length() > 16 * 1024 * 1024
                || targetUid < 0 || targetUid > 0xffff_ffffL) {
            throw new IllegalArgumentException("Invalid bounded native exec-dropper or target identity");
        }
        for (Path path : List.of(supervisorDirectory, manifest, proof, policy, aesKey)) {
            absolute(path);
        }
        if (!proof.getParent().equals(supervisorDirectory) || !policy.getParent().equals(supervisorDirectory)
                || !aesKey.getParent().equals(supervisorDirectory)
                || Set.of(manifest, proof, policy, aesKey).size() != 4) {
            throw new IllegalArgumentException("Fixed handoff leaves must be distinct and inside the supervisor scope");
        }
        if (linuxDeployment != null && linuxDeployment.schema() == 2) {
            requireVersionTwo(linuxDeployment.versionTwo(), os, architecture, dropper, supervisorDirectory, manifest);
        } else if (linuxDeployment != null) {
            var approved = linuxDeployment.files().get(linuxDeployment.isolation().dropper());
            if (!os.equals("linux") || !architecture.equals("x86_64") || !dropper.path().equals(approved.path())
                    || dropper.length() != approved.length() || !dropper.sha256().equals(approved.sha256())) {
                throw new IllegalArgumentException("V2 public dropper/platform differs from the Linux deployment");
            }
            var privateLauncher = Path.of("/etc/bookkeeper/unified-wal/classic-child/launcher");
            for (var file : linuxDeployment.files()) {
                if (file.path().startsWith(supervisorDirectory) || file.path().startsWith(privateLauncher)
                        || file.path().startsWith(Path.of("/run/bookkeeper-uw")) || file.path().equals(manifest)) {
                    throw new IllegalArgumentException("Private material/runtime files cannot enter the program table");
                }
            }
            for (var alias : linuxDeployment.loading().aliases()) {
                if (privateLauncher.startsWith(alias.link()) || alias.link().startsWith(privateLauncher)
                        || supervisorDirectory.startsWith(alias.link())
                        || alias.link().startsWith(supervisorDirectory)) {
                    throw new IllegalArgumentException("System loading aliases cannot cover private supervisor paths");
                }
            }
        }
    }

    private static void requireVersionTwo(SegmentClassicLinuxDeploymentV2 deployment, String os, String architecture,
            SegmentClassicLaunchProgram.File dropper, Path supervisorDirectory, Path manifest) {
        var approved = deployment.files().get(deployment.isolation().dropper());
        if (!os.equals("linux") || !architecture.equals("x86_64")
                || !dropper.path().toString().equals(approved.path()) || dropper.length() != approved.length()
                || !dropper.sha256().equals(approved.sha256())) {
            throw new IllegalArgumentException("Public dropper/platform differs from deployment schema two");
        }
        String privateLauncher = "/etc/bookkeeper/unified-wal/classic-child/launcher";
        String supervisor = supervisorDirectory.toString();
        for (var file : deployment.files()) {
            if (SegmentClassicLinuxDeploymentV2.beneath(file.path(), supervisor)
                    || SegmentClassicLinuxDeploymentV2.beneath(file.path(), privateLauncher)
                    || SegmentClassicLinuxDeploymentV2.beneath(file.path(), "/run/bookkeeper-uw")
                    || file.path().equals(manifest.toString())) {
                throw new IllegalArgumentException("Private material/runtime files cannot enter the program table");
            }
        }
        for (var alias : deployment.loading().aliases()) {
            if (SegmentClassicLinuxDeploymentV2.beneath(privateLauncher, alias.link())
                    || SegmentClassicLinuxDeploymentV2.beneath(alias.link(), privateLauncher)
                    || SegmentClassicLinuxDeploymentV2.beneath(supervisor, alias.link())
                    || SegmentClassicLinuxDeploymentV2.beneath(alias.link(), supervisor)) {
                throw new IllegalArgumentException("System aliases cannot cover private supervisor paths");
            }
        }
    }

    /** Same protected JDK/JAR/JNI files and JVM arguments as child; only this fixed parent entry differs. */
    static SegmentClassicHandoff.Digest parentProgramIdentity(SegmentClassicHandoff.Digest childProgram) {
        return SegmentClassicHandoff.digest(PARENT_DOMAIN, childProgram.value(), MAIN.getBytes(StandardCharsets.UTF_8));
    }

    static SegmentClassicHandoff.Digest parentProgramIdentity(SegmentClassicHandoff.Digest childProgram,
            SegmentClassicLinuxDeployment deployment) throws IOException {
        byte[] main = MAIN.getBytes(StandardCharsets.UTF_8);
        return SegmentClassicHandoff.digest(PARENT_V2_DOMAIN, childProgram.value(),
                ByteBuffer.allocate(4).putInt(main.length).array(), main, deployment.identity().value());
    }

    void requireRecipe(SegmentClassicLaunchTrust.Parsed original) throws IOException {
        var expected = original.recipe();
        if (!recipeId.equals(expected.recipeId()) || !recipe.equals(original.recipeIdentity())
                || !signerKeyId.equals(expected.signerKeyId())
                || !parentProgram.equals(linuxDeployment == null ? parentProgramIdentity(expected.program())
                        : parentProgramIdentity(expected.program(), linuxDeployment))
                || targetUid == expected.oldUid()) {
            throw new IOException("Launcher inputs differ from the original fixed child recipe");
        }
    }

    /** Exact numeric arguments; no shell, account lookup, implicit groups or environment expansion. */
    List<String> childCommand(SegmentClassicLaunchTrust.Parsed original, SegmentClassicLaunchProgram program)
            throws IOException {
        requireRecipe(original);
        if (linuxDeployment != null) {
            throw new IOException("V2 requires its original Linux qualification/cgroup before child creation");
        }
        if (!os.equals(program.os()) || !architecture.equals(program.architecture())) {
            throw new IOException("Exec-dropper platform differs from the fixed child program");
        }
        var expected = original.recipe();
        var command = new ArrayList<>(List.of(dropper.path().toString(), "--uid", Long.toString(expected.oldUid()),
                "--gid", Long.toString(expected.primaryGid()), "--groups", expected.groups().stream()
                        .map(String::valueOf).collect(Collectors.joining(",")), "--"));
        command.addAll(program.command(recipeId));
        return List.copyOf(command);
    }

    static List<String> parentCommand(SegmentClassicLaunchProgram program, String recipeId) {
        var command = new ArrayList<>(program.command(recipeId));
        command.set(command.size() - 2, MAIN);
        return List.copyOf(command);
    }

    static SegmentClassicLauncherInputs decode(byte[] encoded) throws IOException {
        return decodeWithRawPaths(encoded).value();
    }

    record RawPaths(String dropper, String supervisor, String manifest, String proof, String policy, String aesKey) {
        RawPaths {
            for (String path : List.of(dropper, supervisor, manifest, proof, policy, aesKey)) {
                if (path.isEmpty() || path.indexOf(0) >= 0 || path.length() > 4096
                        || !StandardCharsets.UTF_8.newEncoder().canEncode(path)
                        || path.getBytes(StandardCharsets.UTF_8).length > 4096) {
                    throw new IllegalArgumentException("Invalid original launcher path text");
                }
            }
        }

        List<String> values() {
            return List.of(dropper, supervisor, manifest, proof, policy, aesKey);
        }
    }

    /** Raw immutable decode, with typed Path construction deferred until the caller has checked its provider. */
    record Decoded(String recipeId, SegmentClassicHandoff.Digest recipe, SegmentClassicHandoff.Digest signerKeyId,
                   SegmentClassicHandoff.Digest parentProgram, String os, String architecture, RawPaths paths,
                   long dropperLength, SegmentClassicHandoff.Digest dropperHash, long targetUid,
                   SegmentClassicLinuxDeployment linuxDeployment) {
        SegmentClassicLauncherInputs value() throws IOException {
            try {
                return new SegmentClassicLauncherInputs(recipeId, recipe, signerKeyId, parentProgram, os, architecture,
                        new SegmentClassicLaunchProgram.File(Path.of(paths.dropper()),
                                SegmentClassicLaunchProgram.NATIVE,
                                dropperLength, dropperHash), Path.of(paths.supervisor()), targetUid,
                        Path.of(paths.manifest()), Path.of(paths.proof()), Path.of(paths.policy()),
                        Path.of(paths.aesKey()),
                        linuxDeployment);
            } catch (IllegalArgumentException malformed) {
                throw new IOException("Invalid fixed launcher descriptor", malformed);
            }
        }
    }

    static Decoded decodeWithRawPaths(byte[] encoded) throws IOException {
        if (encoded == null || encoded.length < 16 || encoded.length > MAX_V2_BYTES) {
            throw new IOException("Launcher descriptor exceeds its fixed bounds");
        }
        try {
            ByteBuffer b = ByteBuffer.wrap(encoded.clone());
            long magic = b.getLong();
            int schema = Short.toUnsignedInt(b.getShort());
            boolean v2 = magic == MAGIC_V2 && schema == 2;
            if (!(magic == MAGIC && schema == 1 || v2) || b.getShort() != 0
                    || b.getInt() != encoded.length - 16 || !v2 && encoded.length > MAX_BYTES) {
                throw new IllegalArgumentException("Unknown fixed launcher descriptor framing");
            }
            String id = text(b, 128);
            var recipe = digest(b);
            var signer = digest(b);
            var program = digest(b);
            String os = text(b, 16);
            String arch = text(b, 16);
            String dropper = text(b, 4096);
            long length = b.getLong();
            var hash = digest(b);
            if (b.getInt() != (v2 ? 2 : 1)) {
                throw new IllegalArgumentException("Unsupported exec-dropper invocation format");
            }
            String directory = text(b, 4096);
            long uid = Integer.toUnsignedLong(b.getInt());
            String manifest = text(b, 4096);
            String proof = text(b, 4096);
            String policy = text(b, 4096);
            String aesKey = text(b, 4096);
            SegmentClassicLinuxDeployment deployment = null;
            if (v2) {
                int size = b.getInt();
                if (size < 16 || size > b.remaining() || size > MAX_V2_BYTES) {
                    throw new IllegalArgumentException("Invalid bounded Linux deployment body");
                }
                byte[] body = new byte[size];
                b.get(body);
                deployment = SegmentClassicLinuxDeployment.decode(body);
            }
            if (b.hasRemaining()) {
                throw new IllegalArgumentException("Trailing fixed launcher descriptor data");
            }
            return new Decoded(id, recipe, signer, program, os, arch,
                    new RawPaths(dropper, directory, manifest, proof, policy, aesKey), length, hash, uid, deployment);
        } catch (IllegalArgumentException | BufferUnderflowException malformed) {
            throw new IOException("Invalid fixed launcher descriptor", malformed);
        }
    }

    /** Pure canonical bytes only; no approval, signature, file creation or child launch. */
    byte[] encode() throws IOException {
        try {
            boolean v2 = linuxDeployment != null;
            ByteBuffer b = ByteBuffer.allocate(v2 ? MAX_V2_BYTES : MAX_BYTES);
            b.putLong(v2 ? MAGIC_V2 : MAGIC).putShort((short) (v2 ? 2 : 1)).putShort((short) 0).putInt(0);
            putText(b, recipeId);
            b.put(recipe.value()).put(signerKeyId.value()).put(parentProgram.value());
            putText(b, os);
            putText(b, architecture);
            putText(b, dropper.path().toString());
            b.putLong(dropper.length()).put(dropper.sha256().value()).putInt(v2 ? 2 : 1);
            putText(b, supervisorDirectory.toString());
            b.putInt((int) targetUid);
            for (Path path : List.of(manifest, proof, policy, aesKey)) {
                putText(b, path.toString());
            }
            if (v2) {
                byte[] deployment = linuxDeployment.encode();
                b.putInt(deployment.length).put(deployment);
            }
            b.putInt(12, b.position() - 16);
            return Arrays.copyOf(b.array(), b.position());
        } catch (BufferOverflowException malformed) {
            throw new IOException("Launcher descriptor exceeds its canonical encoding bounds", malformed);
        }
    }

    private static void putText(ByteBuffer b, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        b.putShort((short) bytes.length).put(bytes);
    }

    private static SegmentClassicHandoff.Digest digest(ByteBuffer buffer) {
        byte[] value = new byte[32];
        buffer.get(value);
        return new SegmentClassicHandoff.Digest(value);
    }

    private static String text(ByteBuffer buffer, int maximum) throws IOException {
        int length = Short.toUnsignedInt(buffer.getShort());
        if (length < 1 || length > maximum || length > buffer.remaining()) {
            throw new IllegalArgumentException("Invalid fixed launcher text length");
        }
        byte[] value = new byte[length];
        buffer.get(value);
        String decoded = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(value)).toString();
        if (decoded.isBlank() || decoded.indexOf(0) >= 0) {
            throw new IllegalArgumentException("Invalid fixed launcher text");
        }
        return decoded;
    }

    private static void absolute(Path value) {
        Objects.requireNonNull(value, "path");
        if (!value.isAbsolute() || !value.normalize().equals(value) || value.getParent() == null
                || value.toString().getBytes(StandardCharsets.UTF_8).length > 4096
                || value.toString().indexOf(0) >= 0) {
            throw new IllegalArgumentException("Fixed launcher paths must be canonical and absolute");
        }
    }
}

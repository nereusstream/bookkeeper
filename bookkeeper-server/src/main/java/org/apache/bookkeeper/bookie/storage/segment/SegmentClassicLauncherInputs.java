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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Bounded fixed parent input data only. Neither a signer, installed provenance nor process qualification. */
record SegmentClassicLauncherInputs(String recipeId, SegmentClassicHandoff.Digest recipe,
        SegmentClassicHandoff.Digest signerKeyId, SegmentClassicHandoff.Digest parentProgram, String os,
        String architecture, SegmentClassicLaunchProgram.File dropper, Path supervisorDirectory, long targetUid,
        Path manifest, Path proof, Path policy, Path aesKey) {
    static final int MAX_BYTES = 32768;
    static final String MAIN = "org.apache.bookkeeper.server.ControlledClassicLauncherMain";
    private static final long MAGIC = 0x5557434c58303031L; // UWCLX001
    private static final byte[] PARENT_DOMAIN = "BookKeeper/ControlledClassicLauncherProgram/v1\0"
            .getBytes(StandardCharsets.US_ASCII);

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
    }

    /** Same protected JDK/JAR/JNI files and JVM arguments as child; only this fixed parent entry differs. */
    static SegmentClassicHandoff.Digest parentProgramIdentity(SegmentClassicHandoff.Digest childProgram) {
        return SegmentClassicHandoff.digest(PARENT_DOMAIN, childProgram.value(), MAIN.getBytes(StandardCharsets.UTF_8));
    }

    void requireRecipe(SegmentClassicLaunchTrust.Parsed original) throws IOException {
        var expected = original.recipe();
        if (!recipeId.equals(expected.recipeId()) || !recipe.equals(original.recipeIdentity())
                || !signerKeyId.equals(expected.signerKeyId())
                || !parentProgram.equals(parentProgramIdentity(expected.program())) || targetUid == expected.oldUid()) {
            throw new IOException("Launcher inputs differ from the original fixed child recipe");
        }
    }

    /** Exact numeric arguments; no shell, account lookup, implicit groups or environment expansion. */
    List<String> childCommand(SegmentClassicLaunchTrust.Parsed original, SegmentClassicLaunchProgram program)
            throws IOException {
        requireRecipe(original);
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
        if (encoded.length < 16 || encoded.length > MAX_BYTES) {
            throw new IOException("Launcher descriptor exceeds its fixed bounds");
        }
        try {
            ByteBuffer b = ByteBuffer.wrap(encoded.clone());
            if (b.getLong() != MAGIC || b.getShort() != 1 || b.getShort() != 0
                    || b.getInt() != encoded.length - 16) {
                throw new IllegalArgumentException("Unknown fixed launcher descriptor framing");
            }
            String id = text(b, 128);
            var recipe = digest(b);
            var signer = digest(b);
            var program = digest(b);
            String os = text(b, 16);
            String arch = text(b, 16);
            Path dropper = Path.of(text(b, 4096));
            long length = b.getLong();
            var hash = digest(b);
            if (b.getInt() != 1) {
                throw new IllegalArgumentException("Unsupported exec-dropper invocation format");
            }
            Path directory = Path.of(text(b, 4096));
            long uid = Integer.toUnsignedLong(b.getInt());
            var result = new SegmentClassicLauncherInputs(id, recipe, signer, program, os, arch,
                    new SegmentClassicLaunchProgram.File(dropper, SegmentClassicLaunchProgram.NATIVE, length, hash),
                    directory, uid, Path.of(text(b, 4096)), Path.of(text(b, 4096)), Path.of(text(b, 4096)),
                    Path.of(text(b, 4096)));
            if (b.hasRemaining()) {
                throw new IllegalArgumentException("Trailing fixed launcher descriptor data");
            }
            return result;
        } catch (IllegalArgumentException | BufferUnderflowException malformed) {
            throw new IOException("Invalid fixed launcher descriptor", malformed);
        }
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

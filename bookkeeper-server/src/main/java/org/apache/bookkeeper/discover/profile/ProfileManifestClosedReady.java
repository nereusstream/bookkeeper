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
package org.apache.bookkeeper.discover.profile;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.bookkeeper.common.util.nativeio.NativeIO.FileIdentity;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction.FileImage;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction.Image;

/** An inert native seal of the complete staged candidate vector in one manifest CLOSED admission. */
public record ProfileManifestClosedReady(long admissionBirth, String admissionDigest, UUID operation,
        long holderBirth, int holderVersion, UUID firstNonce, UUID nonce, List<Arena> arenas) {
    private static final int MAGIC = 0x55574d52; // UWMR
    private static final int VERSION = 1;
    private static final int FIXED_BYTES = 108;
    private static final int FILE_BYTES = 57;
    private static final int MAX_ARENAS = 64;
    private static final int MAX_FILES = 64;
    public static final int MAX_BYTES = 256 * 1024;
    private static final HexFormat HEX = HexFormat.of();

    public record Arena(FileIdentity stage, List<FileImage> files) {
        public Arena {
            Objects.requireNonNull(stage, "stage");
            requireCount(files.size(), MAX_FILES);
            files = List.copyOf(files);
            for (int i = 0; i < files.size(); i++) {
                var file = files.get(i);
                if (file.index() != i || file.image().object().device() != stage.device()) {
                    throw new IllegalArgumentException("Manifest READY candidate order or device differs");
                }
            }
        }
    }

    public record Receipt(ProfileManifestClosedAdmission.Receipt admitted, ProfileManifestClosedReady ready,
                          long birth, String digest) { }

    public ProfileManifestClosedReady {
        ProfileManifestPreparation.requireBinding(admissionBirth, admissionDigest, operation, firstNonce, nonce);
        if (holderBirth <= admissionBirth || holderVersion < 0 || holderVersion == Integer.MAX_VALUE
                || (holderVersion == 0) != firstNonce.equals(nonce)) {
            throw new IllegalArgumentException("Invalid manifest READY preparation binding");
        }
        requireCount(arenas.size(), MAX_ARENAS);
        arenas = List.copyOf(arenas);
        var objects = new HashSet<FileIdentity>();
        for (var arena : arenas) {
            requireDistinct(objects, arena.stage());
            for (var file : arena.files()) {
                requireDistinct(objects, file.image().object());
            }
        }
    }

    void requireAdmission(ProfileManifestClosedAdmission.Receipt admitted) throws IOException {
        var original = admitted.admission();
        if (admissionBirth != admitted.birth() || !admissionDigest.equals(admitted.digest())
                || !operation.equals(original.operation()) || arenas.size() != original.arenas().size()) {
            throw new IOException("Manifest READY differs from its original admission");
        }
        var originalObjects = new HashSet<FileIdentity>();
        originalObjects.add(original.bookie().root());
        for (var arena : original.arenas()) {
            originalObjects.add(arena.control().root());
            for (var file : arena.files()) {
                originalObjects.add(file.image().object());
            }
        }
        for (int i = 0; i < arenas.size(); i++) {
            var before = original.arenas().get(i);
            var after = arenas.get(i);
            if (after.stage().device() != before.control().root().device()
                    || originalObjects.contains(after.stage()) || after.files().size() != before.files().size()) {
                throw new IOException("Manifest READY does not bind the original Arena geometry");
            }
            for (int j = 0; j < before.files().size(); j++) {
                var originalFile = before.files().get(j).image();
                var candidate = after.files().get(j).image();
                if (originalObjects.contains(candidate.object()) || candidate.length() != originalFile.length()
                        || !candidate.sha256().equals(originalFile.sha256())) {
                    throw new IOException("Manifest READY does not bind the complete fresh DATA bytes");
                }
            }
        }
    }

    public byte[] encode() {
        int length = FIXED_BYTES + 1 + arenas.size() * 17;
        for (var arena : arenas) {
            length += arena.files().size() * FILE_BYTES;
        }
        if (length > MAX_BYTES) {
            throw new IllegalArgumentException("Manifest READY exceeds its native record limit");
        }
        var out = ByteBuffer.allocate(length);
        out.putInt(MAGIC).putInt(VERSION).putLong(admissionBirth).put(HEX.parseHex(admissionDigest));
        putId(out, operation);
        out.putLong(holderBirth).putInt(holderVersion);
        putId(out, firstNonce);
        putId(out, nonce);
        out.put((byte) arenas.size());
        for (var arena : arenas) {
            putObject(out, arena.stage());
            out.put((byte) arena.files().size());
            for (var file : arena.files()) {
                var image = file.image();
                out.put((byte) file.index());
                putObject(out, image.object());
                out.putLong(image.length()).put(HEX.parseHex(image.sha256()));
            }
        }
        return out.array();
    }

    public static ProfileManifestClosedReady decode(byte[] bytes) throws IOException {
        if (bytes.length > MAX_BYTES) {
            throw new IOException("Manifest READY exceeds its native record limit");
        }
        try {
            var in = ByteBuffer.wrap(bytes);
            if (in.getInt() != MAGIC || in.getInt() != VERSION) {
                throw new IOException("Unknown manifest READY format");
            }
            long admitted = in.getLong();
            String digest = getDigest(in);
            var operation = getId(in);
            long holder = in.getLong();
            int version = in.getInt();
            var first = getId(in);
            var nonce = getId(in);
            int count = requireCount(Byte.toUnsignedInt(in.get()), MAX_ARENAS);
            var arenas = new ArrayList<Arena>(count);
            for (int i = 0; i < count; i++) {
                var stage = getObject(in);
                int fileCount = requireCount(Byte.toUnsignedInt(in.get()), MAX_FILES);
                var files = new ArrayList<FileImage>(fileCount);
                for (int j = 0; j < fileCount; j++) {
                    int index = Byte.toUnsignedInt(in.get());
                    files.add(new FileImage(index, new Image(getObject(in), in.getLong(), getDigest(in))));
                }
                arenas.add(new Arena(stage, files));
            }
            if (in.hasRemaining()) {
                throw new IOException("Unexpected manifest READY suffix");
            }
            return new ProfileManifestClosedReady(admitted, digest, operation, holder, version,
                    first, nonce, arenas);
        } catch (BufferUnderflowException | IllegalArgumentException invalid) {
            throw new IOException("Invalid manifest READY record", invalid);
        }
    }

    private static int requireCount(int count, int maximum) {
        if (count < 1 || count > maximum) {
            throw new IllegalArgumentException("Invalid manifest READY vector count");
        }
        return count;
    }

    private static void requireDistinct(HashSet<FileIdentity> objects, FileIdentity object) {
        if (!objects.add(object)) {
            throw new IllegalArgumentException("Manifest READY aliases a stage or candidate object");
        }
    }

    private static String getDigest(ByteBuffer in) {
        byte[] digest = new byte[32];
        in.get(digest);
        return HEX.formatHex(digest);
    }

    private static void putObject(ByteBuffer out, FileIdentity object) {
        out.putLong(object.device()).putLong(object.inode());
    }

    private static FileIdentity getObject(ByteBuffer in) {
        return new FileIdentity(in.getLong(), in.getLong());
    }

    private static void putId(ByteBuffer out, UUID id) {
        out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
    }

    private static UUID getId(ByteBuffer in) {
        return new UUID(in.getLong(), in.getLong());
    }
}

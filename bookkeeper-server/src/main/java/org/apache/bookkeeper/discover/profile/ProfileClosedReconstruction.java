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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.util.nativeio.NativeIO.FileIdentity;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.versioning.LongVersion;

/** Native historical bindings for the single-file CLOSED reconstruction. Neither record admits a writer. */
public final class ProfileClosedReconstruction {
    private static final int ADMISSION = 0x55574341;
    private static final int READY = 0x55574359;
    private static final int VERSION = 1;
    private static final int COHORT_VERSION = 2;
    private static final int MAX_BYTES = 8192;
    public static final int MAX_MEMBERS = 16;

    public record Scope(String bookie, StorageIncarnation incarnation, UUID arena, long epoch) {
        public Scope {
            ProfileAuthorityCodec.text(bookie, 255);
            Objects.requireNonNull(incarnation, "incarnation");
            requireId(arena);
            if (epoch <= 0) {
                throw new IllegalArgumentException("Reconstruction requires a managed epoch");
            }
        }

        String key() throws IOException {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                writeScope(out, this);
            }
            return digest(bytes.toByteArray());
        }
    }

    public record Image(FileIdentity object, long length, String sha256) {
        public Image {
            Objects.requireNonNull(object, "object");
            if (length < 4096 || length % 4096 != 0) {
                throw new IllegalArgumentException("Invalid original DATA extent");
            }
            requireDigest(sha256);
        }
    }

    /** Legacy v1 stores just the ledger ID here, with its complete identity bound by the CLOSED digest. */
    public record Member(long ledger, ProfileLedgerContext context, String closed, String sources) {
        public Member {
            if (ledger < 0 || context != null && context.ledgerId() != ledger) {
                throw new IllegalArgumentException("Invalid CLOSED reconstruction member identity");
            }
            requireDigest(closed);
            requireDigest(sources);
        }

        public boolean matchesContext(ProfileLedgerContext actual) {
            return ledger == actual.ledgerId() && (context == null || context.equals(actual));
        }
    }

    public record Admission(Scope scope, UUID operation, long arenaCut, long bookieCut,
                            long publisherPid, long publisherStartMillis, long publisherSession,
                            String directory, FileIdentity directoryObject, FileIdentity stagingObject,
                            Image original, String dependencies, List<Member> members) {
        public Admission {
            Objects.requireNonNull(scope, "scope");
            requireId(operation);
            Objects.requireNonNull(directoryObject, "directoryObject");
            Objects.requireNonNull(stagingObject, "stagingObject");
            Objects.requireNonNull(original, "original");
            Path root = Path.of(directory);
            if (arenaCut < 0 || bookieCut < 0 || publisherPid <= 0 || publisherStartMillis <= 0
                    || publisherSession == 0 || directory.length() > 4096
                    || !root.isAbsolute() || !root.normalize().equals(root)
                    || directoryObject.device() != stagingObject.device()
                    || directoryObject.device() != original.object().device()
                    || directoryObject.equals(stagingObject) || directoryObject.equals(original.object())
                    || stagingObject.equals(original.object())) {
                throw new IllegalArgumentException("Invalid original reconstruction scope or staging");
            }
            requireDigest(dependencies);
            if (members.isEmpty() || members.size() > MAX_MEMBERS) {
                throw new IllegalArgumentException("Invalid CLOSED reconstruction member count");
            }
            members = List.copyOf(members);
            long previous = -1;
            for (var member : members) {
                if (member.ledger() <= previous || (member.context() == null) != (members.size() == 1)) {
                    throw new IllegalArgumentException("CLOSED reconstruction members are not canonical");
                }
                previous = member.ledger();
            }
        }

        byte[] encode() throws IOException {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(ADMISSION);
                boolean legacy = members.size() == 1;
                out.writeInt(legacy ? VERSION : COHORT_VERSION);
                writeScope(out, scope);
                writeId(out, operation);
                if (legacy) {
                    out.writeLong(members.get(0).ledger());
                }
                out.writeLong(arenaCut);
                out.writeLong(bookieCut);
                out.writeLong(publisherPid);
                out.writeLong(publisherStartMillis);
                out.writeLong(publisherSession);
                out.writeUTF(directory);
                writeObject(out, directoryObject);
                writeObject(out, stagingObject);
                writeImage(out, original);
                writeDigest(out, dependencies);
                if (legacy) {
                    writeDigest(out, members.get(0).closed());
                    writeDigest(out, members.get(0).sources());
                } else {
                    out.writeInt(members.size());
                    for (var member : members) {
                        out.write(member.context().encode());
                        writeDigest(out, member.closed());
                        writeDigest(out, member.sources());
                    }
                }
            }
            if (bytes.size() > MAX_BYTES) {
                throw new IOException("Reconstruction admission exceeds its byte limit");
            }
            return bytes.toByteArray();
        }

        static Admission decode(byte[] bytes) throws IOException {
            try (var in = input(bytes, ADMISSION)) {
                int version = in.readInt();
                if (version != VERSION && version != COHORT_VERSION) {
                    throw new IOException("Unknown reconstruction admission version");
                }
                var scope = new Scope(in.readUTF(), new StorageIncarnation(in.readNBytes(16)), readId(in),
                        in.readLong());
                UUID operation = readId(in);
                long legacyLedger = version == VERSION ? in.readLong() : -1;
                long arenaCut = in.readLong();
                long bookieCut = in.readLong();
                long pid = in.readLong();
                long start = in.readLong();
                long session = in.readLong();
                String directory = in.readUTF();
                var directoryObject = readObject(in);
                var stagingObject = readObject(in);
                var original = readImage(in);
                String dependencies = readDigest(in);
                List<Member> members;
                if (version == VERSION) {
                    members = List.of(new Member(legacyLedger, null, readDigest(in), readDigest(in)));
                } else {
                    int count = in.readInt();
                    if (count < 2 || count > MAX_MEMBERS) {
                        throw new IOException("Invalid reconstruction cohort size");
                    }
                    members = new ArrayList<>(count);
                    for (int index = 0; index < count; index++) {
                        var context = ProfileLedgerContext.decode(ByteBuffer.wrap(
                                in.readNBytes(ProfileLedgerContext.ENCODED_LENGTH)));
                        members.add(new Member(context.ledgerId(), context, readDigest(in), readDigest(in)));
                    }
                }
                requireEnd(in);
                return new Admission(scope, operation, arenaCut, bookieCut, pid, start, session, directory,
                        directoryObject, stagingObject, original, dependencies, members);
            }
        }
    }

    public record Ready(UUID operation, long admissionBirth, String admissionDigest, UUID shadow,
                        long shadowBirth, int shadowVersion, Image candidate) {
        public Ready {
            requireId(operation);
            requireId(shadow);
            if (admissionBirth <= 0 || shadowBirth != admissionBirth || shadowVersion < 1
                    || shadowVersion == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("READY must bind the original admission birth");
            }
            requireDigest(admissionDigest);
            Objects.requireNonNull(candidate, "candidate");
        }

        byte[] encode() throws IOException {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(READY);
                out.writeInt(VERSION);
                writeId(out, operation);
                out.writeLong(admissionBirth);
                writeDigest(out, admissionDigest);
                writeId(out, shadow);
                out.writeLong(shadowBirth);
                out.writeInt(shadowVersion);
                writeImage(out, candidate);
            }
            return bytes.toByteArray();
        }

        static Ready decode(byte[] bytes) throws IOException {
            try (var in = input(bytes, READY)) {
                if (in.readInt() != VERSION) {
                    throw new IOException("Unknown reconstruction READY version");
                }
                var ready = new Ready(readId(in), in.readLong(), readDigest(in), readId(in), in.readLong(),
                        in.readInt(), readImage(in));
                requireEnd(in);
                return ready;
            }
        }
    }

    public record Receipt(Admission admission, long admissionBirth, String admissionDigest,
                          Ready ready, long readyBirth, String readyDigest) { }

    /** One actual native attempt fence, or the original READY that won before this attempt. */
    public record Shadow(Receipt ready, UUID attempt, long birth, int version) { }

    public static String closedDigest(ZKProfileReservationStore.ReadOnlySnapshot snapshot,
            ZKProfileReservationStore.ClosedReconstructionSelf self) throws IOException {
        var metadata = snapshot.closed().metadata();
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.write(snapshot.context().encode());
            out.writeLong(metadata.birthZxid());
            out.writeLong(((LongVersion) metadata.metadata().getVersion()).getLongVersion());
            out.writeLong(metadata.metadata().getValue().getCToken());
            var serialized = new LedgerMetadataSerDe().serialize(metadata.metadata().getValue());
            out.writeInt(serialized.length);
            out.write(serialized);
            out.writeLong(self.commitBirth());
            out.writeLong(self.reference().intentStoreVersion());
            writeId(out, self.reference().intentBirth());
            out.write(self.reference().digest());
            out.writeInt(snapshot.sources().size());
            for (var source : snapshot.sources()) {
                var scope = ProfileAuthorityCodec.encode(new CommittedProfileAuthority(source.scope(),
                        CommittedProfileAuthority.State.PREPARING, source.engine(), 0, true));
                out.writeInt(scope.length);
                out.write(scope);
                out.write(source.capabilityDigest());
                out.writeLong(source.localInstallGeneration());
            }
        }
        return digest(bytes.toByteArray());
    }

    static String digest(byte[] bytes) {
        return HexFormat.of().formatHex(ProfileRepairIntent.digest(bytes));
    }

    private static void writeScope(DataOutputStream out, Scope scope) throws IOException {
        out.writeUTF(scope.bookie());
        out.write(scope.incarnation().value());
        writeId(out, scope.arena());
        out.writeLong(scope.epoch());
    }

    private static void writeId(DataOutputStream out, UUID id) throws IOException {
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
    }

    private static UUID readId(DataInputStream in) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    private static void requireId(UUID id) {
        if (id == null || id.equals(new UUID(0, 0))) {
            throw new IllegalArgumentException("Missing reconstruction identity");
        }
    }

    private static void writeObject(DataOutputStream out, FileIdentity identity) throws IOException {
        out.writeLong(identity.device());
        out.writeLong(identity.inode());
    }

    private static FileIdentity readObject(DataInputStream in) throws IOException {
        return new FileIdentity(in.readLong(), in.readLong());
    }

    private static void writeImage(DataOutputStream out, Image image) throws IOException {
        writeObject(out, image.object());
        out.writeLong(image.length());
        writeDigest(out, image.sha256());
    }

    private static Image readImage(DataInputStream in) throws IOException {
        return new Image(readObject(in), in.readLong(), readDigest(in));
    }

    private static void writeDigest(DataOutputStream out, String hex) throws IOException {
        out.write(HexFormat.of().parseHex(hex));
    }

    private static String readDigest(DataInputStream in) throws IOException {
        return HexFormat.of().formatHex(in.readNBytes(32));
    }

    private static void requireDigest(String hex) {
        if (hex == null || hex.length() != 64 || !HexFormat.of().formatHex(HexFormat.of().parseHex(hex)).equals(hex)) {
            throw new IllegalArgumentException("Invalid reconstruction digest");
        }
    }

    private static DataInputStream input(byte[] bytes, int magic) throws IOException {
        if (bytes.length < 8 || bytes.length > MAX_BYTES) {
            throw new IOException("Invalid reconstruction record extent");
        }
        var in = new DataInputStream(new ByteArrayInputStream(bytes));
        if (in.readInt() != magic) {
            throw new IOException("Unknown reconstruction record");
        }
        return in;
    }

    private static void requireEnd(DataInputStream in) throws IOException {
        if (in.read() != -1) {
            throw new IOException("Unexpected reconstruction record suffix");
        }
    }

    private ProfileClosedReconstruction() { }
}

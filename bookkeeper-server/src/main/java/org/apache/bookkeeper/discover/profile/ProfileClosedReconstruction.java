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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.util.nativeio.NativeIO.FileIdentity;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.versioning.LongVersion;

/** Native historical bindings for the complete CLOSED reconstruction. Neither record admits a writer. */
public final class ProfileClosedReconstruction {
    private static final int ADMISSION = 0x55574341;
    private static final int READY = 0x55574359;
    private static final int VERSION = 1;
    private static final int COHORT_VERSION = 2;
    private static final int MIXED_VERSION = 3;
    private static final int FILE_VECTOR_VERSION = 4;
    private static final int COMMITTED_MOVE_VERSION = 5;
    private static final int RETIRED_VERSION = 6;
    private static final int READY_VECTOR_VERSION = 2;
    private static final int MAX_BYTES = 8192;
    public static final int MAX_MEMBERS = 16;
    private static final int MAX_FILES = 64;

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

    public record FileImage(int index, Image image) {
        public FileImage {
            if (index < 0 || index >= MAX_FILES) {
                throw new IllegalArgumentException("Invalid reconstruction DATA file index");
            }
            Objects.requireNonNull(image, "image");
        }
    }

    public enum Kind { NONEMPTY_SELF, EMPTY_TERMINAL }

    public enum HistoryMode { ORIGINAL, COMMITTED_MOVE, RETIRED }

    /** Legacy v1 stores just the ledger ID here, with its complete identity bound by the CLOSED digest. */
    public record Member(long ledger, ProfileLedgerContext context, String closed, String sources, Kind kind) {
        public Member(long ledger, ProfileLedgerContext context, String closed, String sources) {
            this(ledger, context, closed, sources, Kind.NONEMPTY_SELF);
        }

        public Member {
            Objects.requireNonNull(kind, "kind");
            if (ledger < 0 || context != null && context.ledgerId() != ledger
                    || context == null && kind != Kind.NONEMPTY_SELF) {
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
                            List<FileImage> originals, String dependencies, List<Member> members,
                            HistoryMode historyMode) {
        public Admission(Scope scope, UUID operation, long arenaCut, long bookieCut, long publisherPid,
                long publisherStartMillis, long publisherSession, String directory, FileIdentity directoryObject,
                FileIdentity stagingObject, List<FileImage> originals, String dependencies, List<Member> members,
                boolean committedMoves) {
            this(scope, operation, arenaCut, bookieCut, publisherPid, publisherStartMillis, publisherSession,
                    directory, directoryObject, stagingObject, originals, dependencies, members,
                    committedMoves ? HistoryMode.COMMITTED_MOVE : HistoryMode.ORIGINAL);
        }

        public Admission(Scope scope, UUID operation, long arenaCut, long bookieCut, long publisherPid,
                long publisherStartMillis, long publisherSession, String directory, FileIdentity directoryObject,
                FileIdentity stagingObject, List<FileImage> originals, String dependencies, List<Member> members) {
            this(scope, operation, arenaCut, bookieCut, publisherPid, publisherStartMillis, publisherSession,
                    directory, directoryObject, stagingObject, originals, dependencies, members, false);
        }

        public Admission(Scope scope, UUID operation, long arenaCut, long bookieCut, long publisherPid,
                long publisherStartMillis, long publisherSession, String directory, FileIdentity directoryObject,
                FileIdentity stagingObject, Image original, String dependencies, List<Member> members) {
            this(scope, operation, arenaCut, bookieCut, publisherPid, publisherStartMillis, publisherSession,
                    directory, directoryObject, stagingObject, List.of(new FileImage(0, original)), dependencies,
                    members);
        }

        public Admission {
            Objects.requireNonNull(scope, "scope");
            requireId(operation);
            Objects.requireNonNull(directoryObject, "directoryObject");
            Objects.requireNonNull(stagingObject, "stagingObject");
            Objects.requireNonNull(historyMode, "historyMode");
            originals = canonicalFiles(originals);
            Path root = Path.of(directory);
            if (arenaCut < 0 || bookieCut < 0 || publisherPid <= 0 || publisherStartMillis <= 0
                    || publisherSession == 0 || directory.length() > 4096
                    || !root.isAbsolute() || !root.normalize().equals(root)
                    || directoryObject.device() != stagingObject.device()
                    || directoryObject.equals(stagingObject)) {
                throw new IllegalArgumentException("Invalid original reconstruction scope or staging");
            }
            for (var file : originals) {
                var object = file.image().object();
                if (directoryObject.device() != object.device() || directoryObject.equals(object)
                        || stagingObject.equals(object)) {
                    throw new IllegalArgumentException("Original DATA aliases another reconstruction role");
                }
            }
            requireDigest(dependencies);
            if (members.isEmpty() || members.size() > MAX_MEMBERS) {
                throw new IllegalArgumentException("Invalid CLOSED reconstruction member count");
            }
            members = List.copyOf(members);
            long previous = -1;
            boolean nonempty = false;
            boolean legacy = historyMode == HistoryMode.ORIGINAL && originals.size() == 1 && members.size() == 1;
            for (var member : members) {
                if (member.ledger() <= previous || (member.context() == null) != legacy) {
                    throw new IllegalArgumentException("CLOSED reconstruction members are not canonical");
                }
                nonempty |= member.kind() == Kind.NONEMPTY_SELF;
                previous = member.ledger();
            }
            if (!nonempty) {
                throw new IllegalArgumentException("CLOSED reconstruction requires a nonempty member");
            }
        }

        public Image original() {
            return singleImage(originals);
        }

        /** Whether this record declares the v5 scope with MOVE and no successful FREE. */
        public boolean committedMoves() {
            return historyMode == HistoryMode.COMMITTED_MOVE;
        }

        boolean matchesCandidates(List<FileImage> candidates) {
            if (candidates.size() != originals.size()) {
                return false;
            }
            var originalObjects = new HashSet<FileIdentity>();
            originals.forEach(file -> originalObjects.add(file.image().object()));
            for (int index = 0; index < originals.size(); index++) {
                var original = originals.get(index).image();
                var file = candidates.get(index);
                var candidate = file.image();
                if (file.index() != index || candidate.length() != original.length()
                        || !candidate.sha256().equals(original.sha256())
                        || candidate.object().device() != original.object().device()
                        || originalObjects.contains(candidate.object()) || directoryObject.equals(candidate.object())
                        || stagingObject.equals(candidate.object())) {
                    return false;
                }
            }
            return true;
        }

        byte[] encode() throws IOException {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(ADMISSION);
                boolean vector = historyMode != HistoryMode.ORIGINAL || originals.size() > 1;
                boolean legacy = !vector && members.size() == 1;
                boolean mixed = members.stream().anyMatch(member -> member.kind() == Kind.EMPTY_TERMINAL);
                int version = vector ? FILE_VECTOR_VERSION : legacy ? VERSION : mixed ? MIXED_VERSION : COHORT_VERSION;
                out.writeInt(switch (historyMode) {
                    case ORIGINAL -> version;
                    case COMMITTED_MOVE -> COMMITTED_MOVE_VERSION;
                    case RETIRED -> RETIRED_VERSION;
                });
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
                if (vector) {
                    writeFiles(out, originals);
                } else {
                    writeImage(out, original());
                }
                writeDigest(out, dependencies);
                if (legacy) {
                    writeDigest(out, members.get(0).closed());
                    writeDigest(out, members.get(0).sources());
                } else {
                    out.writeInt(members.size());
                    for (var member : members) {
                        if (mixed || vector) {
                            out.writeByte(member.kind() == Kind.NONEMPTY_SELF ? 1 : 2);
                        }
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
                if (version != VERSION && version != COHORT_VERSION && version != MIXED_VERSION
                        && version != FILE_VECTOR_VERSION && version != COMMITTED_MOVE_VERSION
                        && version != RETIRED_VERSION) {
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
                HistoryMode historyMode = switch (version) {
                    case COMMITTED_MOVE_VERSION -> HistoryMode.COMMITTED_MOVE;
                    case RETIRED_VERSION -> HistoryMode.RETIRED;
                    default -> HistoryMode.ORIGINAL;
                };
                boolean vector = historyMode != HistoryMode.ORIGINAL || version == FILE_VECTOR_VERSION;
                var originals = vector ? readFiles(in, version == FILE_VECTOR_VERSION ? 2 : 1)
                        : List.of(new FileImage(0, readImage(in)));
                String dependencies = readDigest(in);
                List<Member> members;
                if (version == VERSION) {
                    members = List.of(new Member(legacyLedger, null, readDigest(in), readDigest(in)));
                } else {
                    int count = in.readInt();
                    if (count < (vector ? 1 : 2) || count > MAX_MEMBERS) {
                        throw new IOException("Invalid reconstruction cohort size");
                    }
                    members = new ArrayList<>(count);
                    boolean empty = false;
                    for (int index = 0; index < count; index++) {
                        Kind kind = version == MIXED_VERSION || vector ? readKind(in) : Kind.NONEMPTY_SELF;
                        var context = ProfileLedgerContext.decode(ByteBuffer.wrap(
                                in.readNBytes(ProfileLedgerContext.ENCODED_LENGTH)));
                        members.add(new Member(context.ledgerId(), context, readDigest(in), readDigest(in), kind));
                        empty |= kind == Kind.EMPTY_TERMINAL;
                    }
                    if (version == MIXED_VERSION && !empty) {
                        throw new IOException("Mixed reconstruction admission requires an EMPTY member");
                    }
                }
                requireEnd(in);
                return new Admission(scope, operation, arenaCut, bookieCut, pid, start, session, directory,
                        directoryObject, stagingObject, originals, dependencies, members, historyMode);
            }
        }

        private static Kind readKind(DataInputStream in) throws IOException {
            return switch (in.readUnsignedByte()) {
                case 1 -> Kind.NONEMPTY_SELF;
                case 2 -> Kind.EMPTY_TERMINAL;
                default -> throw new IOException("Unknown CLOSED reconstruction member kind");
            };
        }
    }

    public record Ready(UUID operation, long admissionBirth, String admissionDigest, UUID shadow,
                        long shadowBirth, int shadowVersion, List<FileImage> candidates) {
        public Ready(UUID operation, long admissionBirth, String admissionDigest, UUID shadow,
                long shadowBirth, int shadowVersion, Image candidate) {
            this(operation, admissionBirth, admissionDigest, shadow, shadowBirth, shadowVersion,
                    List.of(new FileImage(0, candidate)));
        }

        public Ready {
            requireId(operation);
            requireId(shadow);
            if (admissionBirth <= 0 || shadowBirth != admissionBirth || shadowVersion < 1
                    || shadowVersion == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("READY must bind the original admission birth");
            }
            requireDigest(admissionDigest);
            candidates = canonicalFiles(candidates);
        }

        public Image candidate() {
            return singleImage(candidates);
        }

        byte[] encode() throws IOException {
            var bytes = new ByteArrayOutputStream();
            try (var out = new DataOutputStream(bytes)) {
                out.writeInt(READY);
                out.writeInt(candidates.size() == 1 ? VERSION : READY_VECTOR_VERSION);
                writeId(out, operation);
                out.writeLong(admissionBirth);
                writeDigest(out, admissionDigest);
                writeId(out, shadow);
                out.writeLong(shadowBirth);
                out.writeInt(shadowVersion);
                if (candidates.size() == 1) {
                    writeImage(out, candidate());
                } else {
                    writeFiles(out, candidates);
                }
            }
            return bytes.toByteArray();
        }

        static Ready decode(byte[] bytes) throws IOException {
            try (var in = input(bytes, READY)) {
                int version = in.readInt();
                if (version != VERSION && version != READY_VECTOR_VERSION) {
                    throw new IOException("Unknown reconstruction READY version");
                }
                var operation = readId(in);
                long admissionBirth = in.readLong();
                String admissionDigest = readDigest(in);
                var shadow = readId(in);
                long shadowBirth = in.readLong();
                int shadowVersion = in.readInt();
                var candidates = version == VERSION ? List.of(new FileImage(0, readImage(in))) : readFiles(in, 2);
                requireEnd(in);
                return new Ready(operation, admissionBirth, admissionDigest, shadow, shadowBirth, shadowVersion,
                        candidates);
            }
        }
    }

    public record Receipt(Admission admission, long admissionBirth, String admissionDigest,
                          Ready ready, long readyBirth, String readyDigest) { }

    /** One actual native attempt fence, or the original READY that won before this attempt. */
    public record Shadow(Receipt ready, UUID attempt, long birth, int version) { }

    public static Kind kind(ZKProfileReservationStore.ClosedReconstructionProof proof) {
        return proof instanceof ZKProfileReservationStore.ClosedReconstructionEmpty
                ? Kind.EMPTY_TERMINAL : Kind.NONEMPTY_SELF;
    }

    public static boolean matchesEvidence(Member member, ZKProfileReservationStore.ReadOnlySnapshot snapshot,
            ZKProfileReservationStore.ClosedReconstructionProof proof) throws IOException {
        return member.matchesContext(proof.ledger()) && member.kind() == kind(proof)
                && member.closed().equals(closedDigest(snapshot, proof))
                && (member.kind() != Kind.EMPTY_TERMINAL
                    || member.sources().equals(emptySourceDigest(member.closed())));
    }

    public static String emptySourceDigest(String closed) {
        return digest(("BookKeeper/CLOSEDReconstruction/EMPTY/NoPayloadSources/v1\0" + closed)
                .getBytes(StandardCharsets.US_ASCII));
    }

    public static String closedDigest(ZKProfileReservationStore.ReadOnlySnapshot snapshot,
            ZKProfileReservationStore.ClosedReconstructionProof proof) throws IOException {
        var metadata = snapshot.closed().metadata();
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            if (proof instanceof ZKProfileReservationStore.ClosedReconstructionEmpty) {
                out.write("BookKeeper/CLOSEDReconstruction/EMPTY/Terminal/v1\0".getBytes(StandardCharsets.US_ASCII));
            }
            out.write(snapshot.context().encode());
            out.writeLong(metadata.birthZxid());
            out.writeLong(((LongVersion) metadata.metadata().getVersion()).getLongVersion());
            out.writeLong(metadata.metadata().getValue().getCToken());
            var serialized = new LedgerMetadataSerDe().serialize(metadata.metadata().getValue());
            out.writeInt(serialized.length);
            out.write(serialized);
            if (proof instanceof ZKProfileReservationStore.ClosedReconstructionSelf self) {
                out.writeLong(self.commitBirth());
                out.writeLong(self.reference().intentStoreVersion());
                writeId(out, self.reference().intentBirth());
                out.write(self.reference().digest());
            } else {
                var empty = (ZKProfileReservationStore.ClosedReconstructionEmpty) proof;
                var target = empty.target();
                writeId(out, empty.initial().birthId());
                out.writeLong(empty.initial().storeVersion());
                out.write(empty.initial().digest());
                out.write(snapshot.closed().selection().reference().encode());
                out.writeInt(target.member());
                var scope = ProfileAuthorityCodec.encode(new CommittedProfileAuthority(target.scope(),
                        CommittedProfileAuthority.State.RECOVERY_TERMINAL_CLOSE_AUTHORIZED,
                        EngineProfile.SEGMENT_WAL_ENGINE, 0, true));
                out.writeInt(scope.length);
                out.write(scope);
                out.writeLong(target.terminal().prefixEntryId());
                out.writeLong(target.terminal().prefixLength());
                out.write(target.terminal().selectionDigest());
            }
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

    private static Image singleImage(List<FileImage> files) {
        if (files.size() != 1) {
            throw new IllegalStateException("A reconstruction file vector requires complete verification");
        }
        return files.get(0).image();
    }

    private static List<FileImage> canonicalFiles(List<FileImage> files) {
        if (files.isEmpty() || files.size() > MAX_FILES) {
            throw new IllegalArgumentException("Invalid reconstruction DATA file count");
        }
        files = List.copyOf(files);
        var objects = new HashSet<FileIdentity>();
        for (int index = 0; index < files.size(); index++) {
            if (files.get(index).index() != index || !objects.add(files.get(index).image().object())) {
                throw new IllegalArgumentException("Reconstruction DATA files are not canonical and distinct");
            }
        }
        return files;
    }

    private static void writeFiles(DataOutputStream out, List<FileImage> files) throws IOException {
        out.writeInt(files.size());
        for (var file : files) {
            out.writeInt(file.index());
            writeImage(out, file.image());
        }
    }

    private static List<FileImage> readFiles(DataInputStream in, int minimum) throws IOException {
        int count = in.readInt();
        if (count < minimum || count > MAX_FILES) {
            throw new IOException("Invalid reconstruction DATA file vector count");
        }
        var files = new ArrayList<FileImage>(count);
        for (int index = 0; index < count; index++) {
            files.add(new FileImage(in.readInt(), readImage(in)));
        }
        return files;
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

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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.util.nativeio.NativeIO.FileIdentity;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction.FileImage;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction.HistoryMode;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction.Image;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction.Kind;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction.Member;
import org.apache.bookkeeper.net.BookieId;

/** Inert native evidence from one locked manifest CLOSED audit. */
public record ProfileManifestClosedAdmission(Scope scope, UUID operation, long publisherPid, long publisherStartMillis,
        long publisherSession, ControlPoint bookie, List<Arena> arenas, List<Ledger> ledgers) {
    private static final int MAGIC = 0x55574d41; // UWMA
    private static final int VERSION = 1;
    private static final byte[] SLOT_DOMAIN =
            "BookKeeper/ManifestCLOSEDAdmission/Slot/v1\0".getBytes(StandardCharsets.US_ASCII);
    public static final int MAX_BYTES = 256 * 1024;
    private static final int MAX_ARENAS = 64;
    private static final int MAX_FILES_PER_ARENA = 64;
    private static final int MAX_SHARDS = 64;
    private static final int MAX_BOOKIE_BYTES = 255;
    private static final int DIGEST_BYTES = 32;
    private static final int UUID_BYTES = 2 * Long.BYTES;
    private static final int POINT_BYTES = 5 * Long.BYTES + DIGEST_BYTES;
    private static final int FILE_BYTES = 1 + 3 * Long.BYTES + DIGEST_BYTES;
    private static final int LEDGER_BYTES = ProfileLedgerContext.ENCODED_LENGTH + 3 + 2 * Long.BYTES + 2 * DIGEST_BYTES;
    private static final HexFormat HEX = HexFormat.of();
    private static final List<HistoryMode> HISTORY_TAGS = List.of(HistoryMode.ORIGINAL, HistoryMode.COMMITTED_MOVE,
            HistoryMode.RETIRED, HistoryMode.FULLY_SEALED_REUSE, HistoryMode.RECONSTRUCTED_FULL_REUSE);
    private static final List<Kind> KIND_TAGS = List.of(Kind.NONEMPTY_SELF, Kind.EMPTY_TERMINAL);

    public record Scope(String bookie, StorageIncarnation incarnation, ManifestIdentity manifest,
                        List<ArenaEpoch> arenas) {
        public Scope {
            BookieId.parse(bookie);
            ProfileAuthorityCodec.text(bookie, MAX_BOOKIE_BYTES);
            Objects.requireNonNull(incarnation, "incarnation");
            Objects.requireNonNull(manifest, "manifest");
            requireCount(arenas.size(), MAX_ARENAS);
            arenas = List.copyOf(arenas);
            String previous = "";
            for (var arena : arenas) {
                String id = arena.id().toString();
                if (previous.compareTo(id) >= 0) {
                    throw new IllegalArgumentException("Manifest admission Arena order is not canonical");
                }
                previous = id;
            }
        }

        String key() {
            byte[] name = ProfileAuthorityCodec.text(bookie, MAX_BOOKIE_BYTES);
            var key = ByteBuffer.allocate(SLOT_DOMAIN.length + encodedBytes(name.length));
            key.put(SLOT_DOMAIN);
            write(key, name);
            return ProfileClosedReconstruction.digest(key.array());
        }

        private int encodedBytes(int nameBytes) {
            return Short.BYTES + nameBytes + StorageIncarnation.LENGTH + ManifestIdentity.LENGTH + 1
                    + arenas.size() * (UUID_BYTES + Long.BYTES);
        }

        private void write(ByteBuffer out, byte[] name) {
            out.putShort((short) name.length).put(name).put(incarnation.value()).put(manifest.value())
                    .put((byte) arenas.size());
            for (var arena : arenas) {
                writeId(out, arena.id());
                out.putLong(arena.epoch());
            }
        }

        private static Scope read(ByteBuffer in) {
            int length = requireCount(Short.toUnsignedInt(in.getShort()), MAX_BOOKIE_BYTES);
            String bookie = new String(readBytes(in, length), StandardCharsets.UTF_8);
            var incarnation = new StorageIncarnation(readBytes(in, StorageIncarnation.LENGTH));
            var manifest = new ManifestIdentity(readBytes(in, ManifestIdentity.LENGTH));
            int count = requireCount(readUnsigned(in), MAX_ARENAS);
            var arenas = new ArrayList<ArenaEpoch>(count);
            for (int i = 0; i < count; i++) {
                arenas.add(new ArenaEpoch(readId(in), in.getLong()));
            }
            return new Scope(bookie, incarnation, manifest, arenas);
        }
    }

    public record ArenaEpoch(UUID id, long epoch) {
        public ArenaEpoch {
            requireId(id);
            if (epoch < 0) {
                throw new IllegalArgumentException("Invalid manifest admission epoch");
            }
        }
    }

    public record ControlPoint(FileIdentity root, long cut, long physical, long checkpoint, String history) {
        public ControlPoint {
            Objects.requireNonNull(root, "root");
            if (cut < 0 || physical <= 0 || checkpoint < 0 || history.length() != 2 * DIGEST_BYTES
                    || !HEX.formatHex(HEX.parseHex(history)).equals(history)) {
                throw new IllegalArgumentException("Invalid manifest admission control point");
            }
        }

        private void write(ByteBuffer out) {
            writeObject(out, root);
            out.putLong(cut).putLong(physical).putLong(checkpoint).put(HEX.parseHex(history));
        }

        private static ControlPoint read(ByteBuffer in) {
            return new ControlPoint(readObject(in), in.getLong(), in.getLong(), in.getLong(), readDigest(in));
        }
    }

    public record Arena(ControlPoint control, boolean sessionOpen, HistoryMode mode, List<FileImage> files) {
        public Arena {
            Objects.requireNonNull(control, "control");
            if (!HISTORY_TAGS.contains(mode)) {
                throw new IllegalArgumentException("Invalid manifest admission history mode");
            }
            requireCount(files.size(), MAX_FILES_PER_ARENA);
            files = List.copyOf(files);
            for (int i = 0; i < files.size(); i++) {
                var file = files.get(i);
                if (file.index() != i || file.image().object().device() != control.root().device()) {
                    throw new IllegalArgumentException("Invalid manifest admission DATA file vector");
                }
            }
        }

        private void write(ByteBuffer out) {
            control.write(out);
            out.put((byte) (sessionOpen ? 1 : 0)).put((byte) HISTORY_TAGS.indexOf(mode)).put((byte) files.size());
            for (var file : files) {
                out.put((byte) file.index());
                var image = file.image();
                writeObject(out, image.object());
                out.putLong(image.length()).put(HEX.parseHex(image.sha256()));
            }
        }

        private static Arena read(ByteBuffer in) {
            var control = ControlPoint.read(in);
            int open = readUnsigned(in);
            if (open > 1) {
                throw new IllegalArgumentException("Invalid manifest admission session flag");
            }
            var mode = readTag(in, HISTORY_TAGS);
            int count = requireCount(readUnsigned(in), MAX_FILES_PER_ARENA);
            var files = new ArrayList<FileImage>(count);
            for (int i = 0; i < count; i++) {
                files.add(new FileImage(readUnsigned(in), new Image(readObject(in), in.getLong(), readDigest(in))));
            }
            return new Arena(control, open == 1, mode, files);
        }
    }

    public record Ledger(Member member, int shard, int arena, long lastEntry, long length) {
        public Ledger {
            Objects.requireNonNull(member.context(), "context");
            if (!KIND_TAGS.contains(member.kind()) || shard < 0 || shard >= MAX_SHARDS
                    || arena < 0 || arena >= MAX_ARENAS
                    || (member.kind() == Kind.EMPTY_TERMINAL
                            ? lastEntry != -1 || length != 0 || !member.sources().equals(
                                    ProfileClosedReconstruction.emptySourceDigest(member.closed()))
                            : lastEntry < 0 || length < 0)) {
                throw new IllegalArgumentException("Invalid manifest admission ledger evidence");
            }
        }

        private void write(ByteBuffer out) {
            out.put(member.context().encode()).put((byte) shard).put((byte) arena)
                    .put((byte) KIND_TAGS.indexOf(member.kind())).putLong(lastEntry).putLong(length)
                    .put(HEX.parseHex(member.closed()))
                    .put(HEX.parseHex(member.sources()));
        }

        private static Ledger read(ByteBuffer in) {
            var context = ProfileLedgerContext.decode(in);
            int shard = readUnsigned(in);
            int arena = readUnsigned(in);
            var kind = readTag(in, KIND_TAGS);
            long last = in.getLong();
            long length = in.getLong();
            return new Ledger(new Member(context.ledgerId(), context, readDigest(in), readDigest(in), kind),
                    shard, arena, last, length);
        }
    }

    public record Receipt(ProfileManifestClosedAdmission admission, long birth, String digest) { }

    public ProfileManifestClosedAdmission {
        Objects.requireNonNull(scope, "scope");
        requireId(operation);
        if (publisherPid <= 0 || publisherStartMillis <= 0 || publisherSession == 0
                || arenas.size() != scope.arenas().size()) {
            throw new IllegalArgumentException("Invalid manifest admission publisher or Arena vector");
        }
        arenas = List.copyOf(arenas);
        var objects = new HashSet<FileIdentity>();
        objects.add(bookie.root());
        for (int i = 0; i < arenas.size(); i++) {
            var arena = arenas.get(i);
            if (arena.sessionOpen() && scope.arenas().get(i).epoch() == 0
                    || !objects.add(arena.control().root())) {
                throw new IllegalArgumentException("Invalid manifest admission Arena epoch or root");
            }
            for (var file : arena.files()) {
                if (!objects.add(file.image().object())) {
                    throw new IllegalArgumentException("Manifest admission aliases original DATA objects");
                }
            }
        }
        requireCount(ledgers.size(), ProfileClosedReconstruction.MAX_MEMBERS);
        ledgers = List.copyOf(ledgers);
        int[] routes = new int[MAX_SHARDS];
        Arrays.fill(routes, -1);
        long previous = -1;
        for (var ledger : ledgers) {
            if (ledger.member().ledger() <= previous || ledger.arena() >= arenas.size()
                    || routes[ledger.shard()] >= 0 && routes[ledger.shard()] != ledger.arena()) {
                throw new IllegalArgumentException("Manifest admission ledger vector is not canonical");
            }
            routes[ledger.shard()] = ledger.arena();
            previous = ledger.member().ledger();
        }
    }

    public byte[] encode() {
        byte[] name = ProfileAuthorityCodec.text(scope.bookie(), MAX_BOOKIE_BYTES);
        int length = 2 * Integer.BYTES + scope.encodedBytes(name.length) + UUID_BYTES + 3 * Long.BYTES + POINT_BYTES
                + arenas.size() * (POINT_BYTES + 3) + 1 + ledgers.size() * LEDGER_BYTES;
        for (var arena : arenas) {
            length += arena.files().size() * FILE_BYTES;
        }
        if (length > MAX_BYTES) {
            throw new IllegalArgumentException("Manifest admission exceeds its native record limit");
        }
        var out = ByteBuffer.allocate(length);
        out.putInt(MAGIC).putInt(VERSION);
        scope.write(out, name);
        writeId(out, operation);
        out.putLong(publisherPid).putLong(publisherStartMillis).putLong(publisherSession);
        bookie.write(out);
        for (var arena : arenas) {
            arena.write(out);
        }
        out.put((byte) ledgers.size());
        for (var ledger : ledgers) {
            ledger.write(out);
        }
        return out.array();
    }

    public static ProfileManifestClosedAdmission decode(byte[] bytes) throws IOException {
        if (bytes.length > MAX_BYTES) {
            throw new IOException("Manifest admission exceeds its native record limit");
        }
        try {
            var in = ByteBuffer.wrap(bytes);
            if (in.getInt() != MAGIC || in.getInt() != VERSION) {
                throw new IOException("Unknown manifest admission format");
            }
            var scope = Scope.read(in);
            var operation = readId(in);
            long pid = in.getLong();
            long start = in.getLong();
            long session = in.getLong();
            var bookie = ControlPoint.read(in);
            var arenas = new ArrayList<Arena>(scope.arenas().size());
            for (int i = 0; i < scope.arenas().size(); i++) {
                arenas.add(Arena.read(in));
            }
            int count = requireCount(readUnsigned(in), ProfileClosedReconstruction.MAX_MEMBERS);
            var ledgers = new ArrayList<Ledger>(count);
            for (int i = 0; i < count; i++) {
                ledgers.add(Ledger.read(in));
            }
            if (in.hasRemaining()) {
                throw new IOException("Unexpected manifest admission suffix");
            }
            return new ProfileManifestClosedAdmission(scope, operation, pid, start, session, bookie, arenas, ledgers);
        } catch (BufferUnderflowException | IllegalArgumentException invalid) {
            throw new IOException("Invalid manifest admission", invalid);
        }
    }

    private static int requireCount(int count, int maximum) {
        if (count < 1 || count > maximum) {
            throw new IllegalArgumentException("Invalid manifest admission count");
        }
        return count;
    }

    private static void requireId(UUID id) {
        if (id.getMostSignificantBits() == 0 && id.getLeastSignificantBits() == 0) {
            throw new IllegalArgumentException("Invalid manifest admission identity");
        }
    }

    private static int readUnsigned(ByteBuffer in) {
        return Byte.toUnsignedInt(in.get());
    }

    private static <T> T readTag(ByteBuffer in, List<T> values) {
        int tag = readUnsigned(in);
        if (tag >= values.size()) {
            throw new IllegalArgumentException("Unknown manifest admission tag");
        }
        return values.get(tag);
    }

    private static byte[] readBytes(ByteBuffer in, int length) {
        byte[] bytes = new byte[length];
        in.get(bytes);
        return bytes;
    }

    private static String readDigest(ByteBuffer in) {
        return HEX.formatHex(readBytes(in, DIGEST_BYTES));
    }

    private static FileIdentity readObject(ByteBuffer in) {
        return new FileIdentity(in.getLong(), in.getLong());
    }

    private static void writeObject(ByteBuffer out, FileIdentity object) {
        out.putLong(object.device()).putLong(object.inode());
    }

    private static UUID readId(ByteBuffer in) {
        return new UUID(in.getLong(), in.getLong());
    }

    private static void writeId(ByteBuffer out, UUID id) {
        out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
    }
}

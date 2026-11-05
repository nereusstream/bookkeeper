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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.Signature;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.discover.BookieRouteBinding;
import org.apache.bookkeeper.discover.profile.ZKProfileStartupRequirementsStore;
import org.apache.bookkeeper.net.BookieId;

/** Fixed handoff bytes only. Signature verification is not current isolation, commissioning or a bootstrap session. */
final class SegmentClassicHandoff {
    /** Fixed supervisor identity is independent of a later commissioning operation and process episode. */
    record SupervisorScope(String clusterId, long clusterBirthZxid, BookieId bookieId) {
        SupervisorScope {
            textBytes(clusterId, 1024);
            Objects.requireNonNull(bookieId, "bookieId");
            textBytes(bookieId.toString(), 255);
            if (clusterBirthZxid <= 0) {
                throw new IllegalArgumentException("Original native cluster birth required");
            }
        }
    }

    static final int MAX_BYTES = 65536;
    static final int HEADER_BYTES = 48;
    static final int SIGNATURE_BYTES = 64;
    private static final long PROOF_MAGIC = 0x555743484f463031L; // UWCHOF01
    private static final long POLICY_MAGIC = 0x55574342504f4c31L; // UWCBPOL1
    private static final long INTENT_MAGIC = 0x5557435245544931L; // UWCRETI1
    private static final byte[] SIGNATURE_DOMAIN = "BookKeeper/CommonClassicHandoff/v1\0"
            .getBytes(StandardCharsets.US_ASCII);
    private static final byte[] KEY_DOMAIN = "BookKeeper/CommonClassicAesKey/v1\0"
            .getBytes(StandardCharsets.US_ASCII);
    private static final UUID ZERO = new UUID(0, 0);
    private static final int PURPOSE = 1; // OFFLINE_CLASSIC_BOOTSTRAP only.
    private static final int MAX_MEMBERS = 192 + 1 + 64;
    static final int CLASSIC = 1;
    static final int CONTROL = 2;
    static final int ARENA = 3;

    private SegmentClassicHandoff() { }

    record Digest(byte[] value) {
        Digest {
            value = value.clone();
            if (value.length != 32) {
                throw new IllegalArgumentException("A handoff identity must be 32 bytes");
            }
        }

        @Override
        public byte[] value() {
            return value.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Digest digest && Arrays.equals(value, digest.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }

    /** Generations of the immutable commissioned record, before the mutable native assignment overlay. */
    record Requirements(ZKProfileStartupRequirementsStore.SourceIdentity source, long deviceGeneration,
                        long formatGeneration, long migrationGeneration, long assignmentGeneration,
                        int minimumReader, int minimumWriter) {
        Requirements {
            Objects.requireNonNull(source, "source");
            textBytes(source.nodePath(), 4096);
            if (deviceGeneration < 1 || formatGeneration < 1 || migrationGeneration < 1 || assignmentGeneration < 1
                    || minimumReader < 1 || minimumWriter < 1) {
                throw new IllegalArgumentException("Invalid commissioned handoff generations");
            }
        }

        static Requirements from(ZKProfileStartupRequirementsStore.Observation observation) {
            return from(observation.sourceIdentity(), observation.commissionedCompatibility());
        }

        static Requirements from(ZKProfileStartupRequirementsStore.SourceIdentity source, BookieCompatibilityFact c) {
            return new Requirements(source, c.deviceManifestGeneration().value(),
                    c.localFormatGeneration().value(), c.migrationGeneration().value(),
                    c.effectiveDeleteAssignmentGeneration().value(), c.minimumReaderVersion(),
                    c.minimumWriterVersion());
        }
    }

    /** PID and start time only identify the supervised child; no arbitrary PID adoption is provided. */
    record Episode(UUID channelEpisode, long pid, Instant started, long registrationBirth, long registrationSession) {
        Episode {
            uuid(channelEpisode);
            Objects.requireNonNull(started, "started");
            if (pid < 1 || registrationBirth < 1 || registrationSession == 0) {
                throw new IllegalArgumentException("Invalid supervised episode observation");
            }
        }
    }

    record Operation(String deployment, Digest programConfiguration, Digest signerKeyId,
                     BookieRouteBinding.Observation prepared, Requirements requirements, Episode episode,
                     long oldUid, long targetUid, Digest aesKey, Digest policy) {
        Operation {
            textBytes(deployment, 1024);
            Objects.requireNonNull(programConfiguration, "programConfiguration");
            Objects.requireNonNull(signerKeyId, "signerKeyId");
            Objects.requireNonNull(prepared, "prepared");
            Objects.requireNonNull(requirements, "requirements");
            Objects.requireNonNull(episode, "episode");
            Objects.requireNonNull(aesKey, "aesKey");
            Objects.requireNonNull(policy, "policy");
            uid(oldUid);
            uid(targetUid);
            if (oldUid == 0 || oldUid == targetUid || prepared.binding().phase() != BookieRouteBinding.Phase.PREPARED
                    || prepared.version() != 0) {
                throw new IllegalArgumentException("Original PREPARED and distinct nonprivileged old UID required");
            }
        }
    }

    /** Paths/roles come from the protected manifest. Do not carry a second path list in the proof. */
    record Physical(int kind, int member, long device, long inode) {
        Physical {
            int limit = switch (kind) {
                case CLASSIC -> 192;
                case CONTROL -> 1;
                case ARENA -> 64;
                default -> throw new IllegalArgumentException("Unknown handoff root role");
            };
            if (member < 0 || member >= limit || inode == 0) {
                throw new IllegalArgumentException("Invalid handoff physical member");
            }
        }
    }

    record Member(Physical physical, long uid, long gid, int mode) {
        Member {
            Objects.requireNonNull(physical, "physical");
            SegmentClassicHandoff.uid(uid);
            SegmentClassicHandoff.uid(gid);
            if (mode < 0 || (mode & ~07777) != 0) {
                throw new IllegalArgumentException("Invalid physical root mode");
            }
        }
    }

    /** The fixed child adapter must establish this actual result before the launcher may create a proof. */
    record Terminal(long controlSequence, Instant exited, Digest actualDrainReceipt) {
        Terminal {
            Objects.requireNonNull(exited, "exited");
            Objects.requireNonNull(actualDrainReceipt, "actualDrainReceipt");
            if (controlSequence < 1) {
                throw new IllegalArgumentException("Missing actual terminal control result");
            }
        }
    }

    record Retirement(Operation operation, List<Physical> members) {
        Retirement {
            Objects.requireNonNull(operation, "operation");
            members = List.copyOf(members);
            physicalMembers(members);
        }
    }

    record Proof(Operation operation, List<Member> members, Terminal terminal,
                 Digest unixIsolationReceipt, Digest retirement) {
        Proof {
            Objects.requireNonNull(operation, "operation");
            members = List.copyOf(members);
            Objects.requireNonNull(terminal, "terminal");
            Objects.requireNonNull(unixIsolationReceipt, "unixIsolationReceipt");
            Objects.requireNonNull(retirement, "retirement");
            physicalMembers(members.stream().map(Member::physical).toList());
            if (!retirement.equals(digest(encodeRetirement(new Retirement(operation,
                    members.stream().map(Member::physical).toList()))))) {
                throw new IllegalArgumentException("Proof differs from its original retirement intent");
            }
        }
    }

    record Policy(UUID bindingId, BookieId bookie, StorageIncarnation incarnation,
                  ManifestIdentity manifest, long targetUid) {
        Policy {
            uuid(bindingId);
            Objects.requireNonNull(bookie, "bookie");
            textBytes(bookie.toString(), 255);
            Objects.requireNonNull(incarnation, "incarnation");
            Objects.requireNonNull(manifest, "manifest");
            uid(targetUid);
        }
    }

    static Digest keyIdentity(byte[] rawKey) {
        if (rawKey.length != 32) {
            throw new IllegalArgumentException("Bootstrap requires the separate raw 32-byte AES key");
        }
        return digest(KEY_DOMAIN, rawKey);
    }

    static byte[] encodePolicy(Policy policy) {
        ByteBuffer b = header(ByteBuffer.allocate(512), POLICY_MAGIC);
        b.putInt(PURPOSE);
        putUuid(b, policy.bindingId());
        putText(b, policy.bookie().toString(), 255);
        b.put(policy.incarnation().value()).put(policy.manifest().value()).putInt((int) policy.targetUid());
        return bytes(b);
    }

    static Policy decodePolicy(byte[] encoded) throws IOException {
        try {
            byte[] stable = stable(encoded, 512);
            ByteBuffer b = input(stable, 512, POLICY_MAGIC);
            constant(b.getInt(), PURPOSE);
            var policy = new Policy(getUuid(b), BookieId.parse(getText(b, 255)),
                    new StorageIncarnation(fixed(b, 16)), new ManifestIdentity(fixed(b, 32)), getUid(b));
            canonical(b, stable, encodePolicy(policy));
            return policy;
        } catch (IllegalArgumentException | BufferUnderflowException malformed) {
            throw new IOException("Invalid exact bootstrap policy", malformed);
        }
    }

    static byte[] encodeRetirement(Retirement retirement) {
        ByteBuffer b = header(ByteBuffer.allocate(MAX_BYTES), INTENT_MAGIC);
        putOperation(b, retirement.operation());
        b.putShort((short) retirement.members().size());
        for (Physical member : retirement.members()) {
            putPhysical(b, member);
        }
        return bytes(b);
    }

    static Retirement decodeRetirement(byte[] encoded) throws IOException {
        try {
            byte[] stable = stable(encoded, MAX_BYTES);
            ByteBuffer b = input(stable, MAX_BYTES, INTENT_MAGIC);
            Operation operation = getOperation(b);
            int count = count(b);
            var members = new ArrayList<Physical>(count);
            for (int member = 0; member < count; member++) {
                members.add(getPhysical(b));
            }
            var retirement = new Retirement(operation, members);
            canonical(b, stable, encodeRetirement(retirement));
            return retirement;
        } catch (IllegalArgumentException | BufferUnderflowException | DateTimeException malformed) {
            throw new IOException("Invalid fixed retirement intent", malformed);
        }
    }

    /** No signing API: only the actual launcher owns a private key and can establish the facts it signs. */
    static byte[] signingMessage(Proof proof) {
        byte[] unsigned = unsigned(proof);
        return ByteBuffer.allocate(SIGNATURE_DOMAIN.length + unsigned.length)
                .put(SIGNATURE_DOMAIN).put(unsigned).array();
    }

    static byte[] envelope(Proof proof, byte[] signature) {
        if (signature.length != SIGNATURE_BYTES) {
            throw new IllegalArgumentException("Fixed Ed25519 signature length required");
        }
        byte[] unsigned = unsigned(proof);
        return ByteBuffer.allocate(unsigned.length + SIGNATURE_BYTES).put(unsigned).put(signature).array();
    }

    /** Verifies original bytes before parsing a payload. Returns data, never an adoptable session. */
    static Proof verify(byte[] encoded, Digest expectedSigner, PublicKey trustedKey) throws IOException {
        if (encoded.length < HEADER_BYTES + SIGNATURE_BYTES || encoded.length > MAX_BYTES) {
            throw new IOException("Handoff envelope length exceeds fixed bounds");
        }
        byte[] stable = encoded.clone();
        try {
            ByteBuffer b = input(stable, MAX_BYTES, PROOF_MAGIC);
            int length = b.getInt();
            if (length < 0 || length != stable.length - HEADER_BYTES - SIGNATURE_BYTES
                    || !expectedSigner.equals(new Digest(fixed(b, 32)))) {
                throw new IllegalArgumentException("Handoff length or trusted signer mismatch");
            }
            int unsignedLength = HEADER_BYTES + length;
            Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(trustedKey);
            verifier.update(SIGNATURE_DOMAIN);
            verifier.update(stable, 0, unsignedLength);
            if (!verifier.verify(stable, unsignedLength, SIGNATURE_BYTES)) {
                throw new IOException("Handoff Ed25519 signature rejected");
            }
            b.limit(unsignedLength);
            Operation operation = getOperation(b);
            int count = count(b);
            var members = new ArrayList<Member>(count);
            for (int member = 0; member < count; member++) {
                members.add(new Member(getPhysical(b), getUid(b), getUid(b), b.getInt()));
            }
            constant(b.getInt(), 1); // Actual adapter DRAINED result, attested by the launcher.
            constant(b.getInt(), 0); // Normal successful process exit, never killed/unknown.
            Terminal terminal = new Terminal(b.getLong(), getInstant(b), getDigest(b));
            constant(b.getInt(), 1); // Complete old-denied/new-required-access result, attested by the launcher.
            Proof proof = new Proof(operation, members, terminal, getDigest(b), getDigest(b));
            canonical(b, Arrays.copyOf(stable, unsignedLength), unsigned(proof));
            return proof;
        } catch (IllegalArgumentException | BufferUnderflowException | DateTimeException
                | GeneralSecurityException bad) {
            throw new IOException("Invalid fixed handoff proof", bad);
        }
    }

    static void requireMaterials(Proof proof, Policy policy, byte[] rawKey, SegmentArenaManifest manifest)
            throws IOException {
        var operation = proof.operation();
        var nativeScope = operation.prepared().binding();
        if (rawKey.length != 32) {
            throw new IOException("Separate raw 32-byte AES material required");
        }
        Policy expected = new Policy(nativeScope.bindingId(), nativeScope.bookieId(), nativeScope.incarnation(),
                nativeScope.manifest(), operation.targetUid());
        if (!expected.equals(policy) || !operation.policy().equals(digest(encodePolicy(policy)))
                || !operation.aesKey().equals(keyIdentity(rawKey)) || manifest.classicRoots().isEmpty()
                || !nativeScope.manifest().equals(manifest.identity())
                || !nativeScope.bookieId().equals(manifest.bookieId())
                || !nativeScope.incarnation().equals(manifest.incarnation())
                || !nativeScope.controlDirectory().equals(manifest.controlDirectory().toString())) {
            throw new IOException("Handoff material or complete manifest scope mismatch");
        }
        var expectedMembers = new ArrayList<String>();
        for (int member = 0; member < manifest.classicRoots().size(); member++) {
            expectedMembers.add(CLASSIC + ":" + member);
        }
        expectedMembers.add(CONTROL + ":0");
        for (int member = 0; member < manifest.arenas().size(); member++) {
            expectedMembers.add(ARENA + ":" + member);
        }
        if (!expectedMembers.equals(proof.members().stream().map(member ->
                member.physical().kind() + ":" + member.physical().member()).toList())) {
            throw new IOException("Handoff omits a required physical manifest member");
        }
    }

    private static byte[] unsigned(Proof proof) {
        ByteBuffer b = header(ByteBuffer.allocate(MAX_BYTES - SIGNATURE_BYTES), PROOF_MAGIC);
        b.putInt(0).put(proof.operation().signerKeyId().value);
        putOperation(b, proof.operation());
        b.putShort((short) proof.members().size());
        for (Member member : proof.members()) {
            putPhysical(b, member.physical());
            b.putInt((int) member.uid()).putInt((int) member.gid()).putInt(member.mode());
        }
        b.putInt(1).putInt(0).putLong(proof.terminal().controlSequence());
        putInstant(b, proof.terminal().exited());
        b.put(proof.terminal().actualDrainReceipt().value).putInt(1)
                .put(proof.unixIsolationReceipt().value).put(proof.retirement().value);
        b.putInt(12, b.position() - HEADER_BYTES);
        return bytes(b);
    }

    private static void putOperation(ByteBuffer b, Operation operation) {
        b.putInt(PURPOSE);
        putText(b, operation.deployment(), 1024);
        b.put(operation.programConfiguration().value).put(operation.signerKeyId().value);
        var observed = operation.prepared();
        byte[] nativeBytes = observed.binding().encode();
        b.putInt(nativeBytes.length).put(nativeBytes);
        putUuid(b, observed.birthId());
        b.putLong(observed.birthZxid()).putLong(observed.modifiedZxid()).putInt(observed.version());
        Requirements r = operation.requirements();
        var source = r.source();
        putText(b, source.nodePath(), 4096);
        putUuid(b, source.birthId());
        b.putLong(source.birthZxid()).putLong(source.modifiedZxid()).putInt(source.version()).put(source.sha256());
        b.putLong(r.deviceGeneration()).putLong(r.formatGeneration()).putLong(r.migrationGeneration())
                .putLong(r.assignmentGeneration()).putInt(r.minimumReader()).putInt(r.minimumWriter());
        Episode episode = operation.episode();
        putUuid(b, episode.channelEpisode());
        b.putLong(episode.pid());
        putInstant(b, episode.started());
        b.putLong(episode.registrationBirth()).putLong(episode.registrationSession());
        b.putInt((int) operation.oldUid()).putInt((int) operation.targetUid())
                .put(operation.aesKey().value).put(operation.policy().value);
    }

    private static Operation getOperation(ByteBuffer b) {
        constant(b.getInt(), PURPOSE);
        String deployment = getText(b, 1024);
        Digest program = getDigest(b);
        Digest signer = getDigest(b);
        int length = b.getInt();
        if (length < 1 || length > BookieRouteBinding.MAX_BYTES) {
            throw new IllegalArgumentException("Invalid original native binding length");
        }
        var binding = BookieRouteBinding.decode(fixed(b, length));
        var observed = new BookieRouteBinding.Observation(binding, getUuid(b), b.getLong(), b.getLong(), b.getInt());
        var source = new ZKProfileStartupRequirementsStore.SourceIdentity(getText(b, 4096), getUuid(b),
                b.getLong(), b.getLong(), b.getInt(), fixed(b, 32));
        var requirements = new Requirements(source, b.getLong(), b.getLong(), b.getLong(), b.getLong(),
                b.getInt(), b.getInt());
        var episode = new Episode(getUuid(b), b.getLong(), getInstant(b), b.getLong(), b.getLong());
        return new Operation(deployment, program, signer, observed, requirements, episode,
                getUid(b), getUid(b), getDigest(b), getDigest(b));
    }

    private static int count(ByteBuffer b) {
        int count = Short.toUnsignedInt(b.getShort());
        if (count < 3 || count > MAX_MEMBERS) {
            throw new IllegalArgumentException("Handoff physical member count exceeds its fixed bounds");
        }
        return count;
    }

    private static void physicalMembers(List<Physical> members) {
        if (members.isEmpty() || members.size() > MAX_MEMBERS) {
            throw new IllegalArgumentException("Handoff physical members exceed bounds");
        }
        int previousKind = 0;
        int previousMember = -1;
        int roles = 0;
        var identities = new HashSet<List<Long>>();
        for (Physical member : members) {
            if (member.kind() < previousKind
                    || member.member() != (member.kind() == previousKind ? previousMember + 1 : 0)
                    || !identities.add(List.of(member.device(), member.inode()))) {
                throw new IllegalArgumentException("Handoff members are aliased, repeated or noncanonical");
            }
            previousKind = member.kind();
            previousMember = member.member();
            roles |= 1 << member.kind();
        }
        if (roles != (1 << CLASSIC | 1 << CONTROL | 1 << ARENA)) {
            throw new IllegalArgumentException("Handoff requires Classic, control and Arena members");
        }
    }

    private static void putPhysical(ByteBuffer b, Physical member) {
        b.putShort((short) member.kind()).putShort((short) member.member())
                .putLong(member.device()).putLong(member.inode());
    }

    private static Physical getPhysical(ByteBuffer b) {
        return new Physical(Short.toUnsignedInt(b.getShort()), Short.toUnsignedInt(b.getShort()),
                b.getLong(), b.getLong());
    }

    private static ByteBuffer header(ByteBuffer b, long magic) {
        return b.putLong(magic).putShort((short) 1).putShort((short) 0);
    }

    private static ByteBuffer input(byte[] bytes, int maximum, long magic) {
        if (bytes.length < 12 || bytes.length > maximum) {
            throw new IllegalArgumentException("Invalid fixed handoff component length");
        }
        ByteBuffer b = ByteBuffer.wrap(bytes);
        if (b.getLong() != magic || b.getShort() != 1 || b.getShort() != 0) {
            throw new IllegalArgumentException("Unknown fixed handoff component schema or flags");
        }
        return b;
    }

    private static byte[] stable(byte[] bytes, int maximum) {
        if (bytes.length < 12 || bytes.length > maximum) {
            throw new IllegalArgumentException("Invalid fixed handoff component length");
        }
        return bytes.clone();
    }

    private static byte[] bytes(ByteBuffer b) {
        return Arrays.copyOf(b.array(), b.position());
    }

    private static void canonical(ByteBuffer b, byte[] original, byte[] expected) {
        if (b.hasRemaining() || !Arrays.equals(original, expected)) {
            throw new IllegalArgumentException("Noncanonical fixed handoff bytes");
        }
    }

    private static byte[] textBytes(String value, int maximum) {
        Objects.requireNonNull(value, "text");
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < 1 || bytes.length > maximum || value.indexOf(0) >= 0
                || !StandardCharsets.UTF_8.newEncoder().canEncode(value)) {
            throw new IllegalArgumentException("Invalid fixed handoff text");
        }
        return bytes;
    }

    private static void putText(ByteBuffer b, String value, int maximum) {
        byte[] bytes = textBytes(value, maximum);
        b.putInt(bytes.length).put(bytes);
    }

    private static String getText(ByteBuffer b, int maximum) {
        int length = b.getInt();
        if (length < 1 || length > maximum) {
            throw new IllegalArgumentException("Invalid fixed handoff text length");
        }
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(fixed(b, length))).toString();
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException("Invalid handoff UTF-8", malformed);
        }
    }

    private static byte[] fixed(ByteBuffer b, int length) {
        if (length < 0 || length > b.remaining()) {
            throw new IllegalArgumentException("Truncated fixed handoff field");
        }
        byte[] value = new byte[length];
        b.get(value);
        return value;
    }

    private static Digest getDigest(ByteBuffer b) {
        return new Digest(fixed(b, 32));
    }

    private static void putUuid(ByteBuffer b, UUID value) {
        b.putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits());
    }

    private static UUID getUuid(ByteBuffer b) {
        UUID value = new UUID(b.getLong(), b.getLong());
        uuid(value);
        return value;
    }

    private static void uuid(UUID value) {
        if (Objects.requireNonNull(value, "uuid").equals(ZERO)) {
            throw new IllegalArgumentException("Zero handoff operation identity");
        }
    }

    private static void putInstant(ByteBuffer b, Instant instant) {
        b.putLong(instant.getEpochSecond()).putInt(instant.getNano());
    }

    private static Instant getInstant(ByteBuffer b) {
        long seconds = b.getLong();
        int nanos = b.getInt();
        if (nanos < 0 || nanos > 999999999) {
            throw new IllegalArgumentException("Noncanonical handoff timestamp");
        }
        return Instant.ofEpochSecond(seconds, nanos);
    }

    private static long getUid(ByteBuffer b) {
        return Integer.toUnsignedLong(b.getInt());
    }

    private static void uid(long value) {
        if (value < 0 || value > 0xffff_ffffL) {
            throw new IllegalArgumentException("Invalid Unix credential identity");
        }
    }

    private static void constant(int actual, int expected) {
        if (actual != expected) {
            throw new IllegalArgumentException("Unsupported fixed handoff purpose or terminal result");
        }
    }

    static Digest digest(byte[]... inputs) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (byte[] input : inputs) {
                digest.update(input);
            }
            return new Digest(digest.digest());
        } catch (GeneralSecurityException unavailable) {
            throw new IllegalStateException("Handoff digest is unavailable", unavailable);
        }
    }
}

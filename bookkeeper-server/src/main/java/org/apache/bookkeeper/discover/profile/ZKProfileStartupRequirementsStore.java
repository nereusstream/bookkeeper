/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.common.profile.startup.BookieCompatibilityFact;
import org.apache.bookkeeper.common.profile.startup.ProfileReadinessRecord;
import org.apache.bookkeeper.common.profile.startup.RequiredDeviceManifest;
import org.apache.bookkeeper.common.profile.startup.StartupReadinessInput;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

/** Immutable initial requirements in an explicitly provisioned namespace; not an old-binary/OS credential fence. */
public final class ZKProfileStartupRequirementsStore {
    public enum Status { FOUND, NOT_FOUND, UNAVAILABLE, CORRUPT }
    public enum ProvisionStatus { APPLIED, ALREADY_APPLIED, CONFLICT, UNAVAILABLE, RESPONSE_LOST }
    public record ReadResult(Status status, Optional<Observation> observation) { }

    /** Only this store's verified persistent bytes can create an observation. */
    public final class Observation {
        private final ProfileStartupRequirementsCodec.Envelope envelope;
        private final long birthZxid;
        private final long modifiedZxid;

        private Observation(ProfileStartupRequirementsCodec.Envelope envelope, long birthZxid, long modifiedZxid) {
            this.envelope = envelope;
            this.birthZxid = birthZxid;
            this.modifiedZxid = modifiedZxid;
        }

        private ZKProfileStartupRequirementsStore owner() {
            return ZKProfileStartupRequirementsStore.this;
        }

        public BookieCompatibilityFact compatibility() {
            return envelope.compatibility();
        }
        public RequiredDeviceManifest requiredDevices() {
            return envelope.devices();
        }

        /** Actual graph replaces recovery/device observations later; scope/credential proof is external. */
        public StartupReadinessInput applyTo(StartupReadinessInput requested) {
            return new StartupReadinessInput(compatibility(), requiredDevices(), requested.recoveryFacts(),
                    requested.newScopeFallback(), requested.rollbackSafetyFacts(),
                    requested.targetReadinessGeneration(),
                    requested.capabilityGeneration(), requested.runtimeReaderVersion(),
                    requested.runtimeWriterVersion(),
                    requested.supportedMandatoryFeatures(), requested.profileEndpoint(), requested.capabilityHints());
        }
    }

    private final ZooKeeper zk;
    private final ProfileZKReadClient reads;
    private final String root;
    private final List<ACL> acls;
    private final List<ACL> markerAcls;
    private final long timeoutNanos;

    public ZKProfileStartupRequirementsStore(ZooKeeper zk, String root, List<ACL> acls, Duration timeout) {
        this.zk = Objects.requireNonNull(zk, "zk");
        reads = new ProfileZKReadClient(zk);
        PathUtils.validatePath(root);
        if (root.equals("/") || acls.isEmpty() || timeout.isZero() || timeout.isNegative()
                || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Invalid initial startup namespace/ACLs/timeout");
        }
        this.root = root;
        this.acls = acls.stream().map(a -> new ACL(a.getPerms(), new Id(a.getId().getScheme(), a.getId().getId())))
                .toList();
        markerAcls = this.acls.stream().filter(a -> (a.getPerms() & org.apache.zookeeper.ZooDefs.Perms.READ) != 0)
                .map(a -> new ACL(org.apache.zookeeper.ZooDefs.Perms.READ,
                        new Id(a.getId().getScheme(), a.getId().getId()))).toList();
        if (markerAcls.isEmpty()) {
            throw new IllegalArgumentException("Startup birth marker requires reader ACL");
        }
        timeoutNanos = timeout.toNanos();
    }

    /** Explicit create-only provisioning. An existing missing/corrupt record is never overwritten or repaired. */
    public ProvisionStatus provision(BookieCompatibilityFact compatibility, RequiredDeviceManifest devices) {
        byte[] bytes = ProfileStartupRequirementsCodec.encode(
                new ProfileStartupRequirementsCodec.Envelope(UUID.randomUUID(), compatibility, devices));
        long deadline = System.nanoTime() + timeoutNanos;
        boolean submitted = false;
        try {
            var existing = snapshot(compatibility.bookieId(), deadline);
            if (existing != null) {
                return existing.compatibility().equals(compatibility) && existing.requiredDevices().equals(devices)
                        ? ProvisionStatus.ALREADY_APPLIED : ProvisionStatus.CONFLICT;
            }
            var envelope = ProfileStartupRequirementsCodec.decode(bytes);
            ProfileZKReadClient.requireTime(deadline);
            var reply = new CompletableFuture<Integer>();
            submitted = true;
            zk.multi(List.of(Op.create(path(compatibility.bookieId()), bytes, acls, CreateMode.PERSISTENT),
                    Op.create(marker(envelope), new byte[0], markerAcls, CreateMode.PERSISTENT)),
                    (code, ignored, context, results) -> reply.complete(code), null);
            int code = ProfileZKReadClient.await(reply, deadline);
            if (code == Code.OK.intValue()) {
                return ProvisionStatus.APPLIED;
            }
            if (code == Code.NODEEXISTS.intValue()) {
                return ProvisionStatus.CONFLICT;
            }
            return ZKProfileRegistrationStore.knownRejection(code)
                    ? ProvisionStatus.UNAVAILABLE : ProvisionStatus.RESPONSE_LOST;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return submitted ? ProvisionStatus.RESPONSE_LOST : ProvisionStatus.UNAVAILABLE;
        } catch (TimeoutException unavailable) {
            return submitted ? ProvisionStatus.RESPONSE_LOST : ProvisionStatus.UNAVAILABLE;
        } catch (IllegalArgumentException | IllegalStateException corrupt) {
            return ProvisionStatus.UNAVAILABLE;
        }
    }

    public ReadResult read(String bookieId) {
        try {
            var observed = snapshot(bookieId, System.nanoTime() + timeoutNanos);
            return new ReadResult(observed == null ? Status.NOT_FOUND : Status.FOUND, Optional.ofNullable(observed));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new ReadResult(Status.UNAVAILABLE, Optional.empty());
        } catch (TimeoutException | IllegalStateException unavailable) {
            return new ReadResult(Status.UNAVAILABLE, Optional.empty());
        } catch (IllegalArgumentException corrupt) {
            return new ReadResult(Status.CORRUPT, Optional.empty());
        }
    }

    void requirePublisherClient(ZooKeeper client, Observation observed, String bookieId) {
        if (zk != client || observed == null || !observed.compatibility().bookieId().equals(bookieId)) {
            throw new IllegalArgumentException("Startup requirement source/client/scope mismatch");
        }
        // Also rejects an observation constructed by another instance, including one sharing this client.
        if (observed.owner() != this) {
            throw new IllegalArgumentException("Startup observation belongs to another source");
        }
    }

    List<Op> publicationChecks(Observation expected, ProfileReadinessRecord readiness, long deadline)
            throws InterruptedException, TimeoutException {
        var current = snapshot(expected.compatibility().bookieId(), deadline);
        if (current == null || expected.owner() != this || current.birthZxid != expected.birthZxid
                || current.modifiedZxid != expected.modifiedZxid || !current.envelope.equals(expected.envelope)) {
            throw new IllegalStateException("Required startup source changed or disappeared");
        }
        var c = expected.compatibility();
        if (c.migrationState() != BookieCompatibilityFact.MigrationState.FORMAT_READY
                || !c.bookieId().equals(readiness.bookieId())
                || !c.storageIncarnation().equals(readiness.storageIncarnation())
                || c.activeEngine() != readiness.engine()
                || c.profileWireMajor() != readiness.protocolMajor()
                || c.profileWireMinor() != readiness.protocolMinor()
                || !c.localFormatGeneration().equals(readiness.localFormatGeneration())
                || !c.deviceManifestIdentity().equals(readiness.deviceManifestIdentity())
                || !c.deviceManifestGeneration().equals(readiness.deviceManifestGeneration())
                || !c.effectiveDeleteAssignmentGeneration().equals(readiness.effectiveAssignmentGeneration())
                || c.minimumReaderVersion() != readiness.minimumReaderVersion()
                || c.minimumWriterVersion() != readiness.minimumWriterVersion()) {
            throw new IllegalArgumentException("Readiness does not consume the required startup scope");
        }
        return List.of(Op.check(marker(expected.envelope), 0), Op.check(path(c.bookieId()), 0));
    }

    private Observation snapshot(String bookieId, long deadline) throws InterruptedException, TimeoutException {
        if (reads.sync(root, deadline) != Code.OK.intValue()) {
            throw new IllegalStateException("Startup requirement sync unavailable");
        }
        var node = reads.get(path(bookieId), deadline);
        if (node.code() == Code.NONODE.intValue()) {
            return null;
        }
        if (node.code() != Code.OK.intValue()) {
            throw new IllegalStateException("Startup requirement read unavailable");
        }
        var envelope = ProfileStartupRequirementsCodec.decode(node.bytes());
        var stat = node.stat();
        if (stat == null || stat.getVersion() != 0 || stat.getEphemeralOwner() != 0
                || !envelope.compatibility().bookieId().equals(bookieId)) {
            throw new IllegalArgumentException("Invalid immutable startup requirement node");
        }
        var anchor = reads.get(marker(envelope), deadline);
        if (anchor.code() != Code.OK.intValue()) {
            if (anchor.code() == Code.NONODE.intValue()) {
                throw new IllegalArgumentException("Required startup birth marker missing");
            }
            throw new IllegalStateException("Startup requirement anchor unavailable");
        }
        if (anchor.stat() == null || anchor.stat().getVersion() != 0 || anchor.stat().getEphemeralOwner() != 0
                || anchor.bytes() == null || anchor.bytes().length != 0
                || anchor.stat().getCzxid() != stat.getCzxid()) {
            throw new IllegalArgumentException("Invalid startup birth marker");
        }
        return new Observation(envelope, stat.getCzxid(), stat.getMzxid());
    }

    String path(String bookieId) {
        byte[] name = Objects.requireNonNull(bookieId, "bookieId").getBytes(StandardCharsets.UTF_8);
        if (name.length < 1 || name.length > BookieCompatibilityFact.MAX_BOOKIE_ID_BYTES
                || !bookieId.equals(new String(name, StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("Invalid startup Bookie identity");
        }
        return root + "/" + Base64.getUrlEncoder().withoutPadding().encodeToString(name);
    }

    private String marker(ProfileStartupRequirementsCodec.Envelope envelope) {
        return path(envelope.compatibility().bookieId()) + "/birth-" + envelope.birth();
    }
}

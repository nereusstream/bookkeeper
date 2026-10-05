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

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.control.ProfileAuthorityKey;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore.ReadStatus;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

/**
 * Experimental immutable PREPARING publication and INSTALL cold authority projection.
 * Caller owns the provisioned namespace/ACLs and client. Invoke on a bounded cold caller, not an event or DATA owner.
 * No READY, mutation CAS, retry, orphan GC, local install, activation or normal writer is supplied.
 */
public final class ZKProfileReservationStore {
    public enum CreateStatus { APPLIED, ALREADY_APPLIED, CONFLICT, QUARANTINED, DURABILITY_UNKNOWN, UNAVAILABLE }

    public record Snapshot(long storeVersion, ProfileReservation reservation) {
        public Snapshot {
            Objects.requireNonNull(reservation, "reservation");
            if (storeVersion < 0) {
                throw new IllegalArgumentException("Invalid reservation store version");
            }
        }
    }

    public record ReadResult(ReadStatus status, Snapshot snapshot) {
        public ReadResult {
            Objects.requireNonNull(status, "status");
            if ((status == ReadStatus.FOUND) != (snapshot != null)) {
                throw new IllegalArgumentException("Only FOUND carries a reservation snapshot");
            }
        }
    }

    public record CreateResult(CreateStatus status, Snapshot snapshot) {
        public CreateResult {
            Objects.requireNonNull(status, "status");
            if ((status == CreateStatus.APPLIED || status == CreateStatus.ALREADY_APPLIED) != (snapshot != null)) {
                throw new IllegalArgumentException("Only proved creation carries a reservation snapshot");
            }
        }
    }

    private final ZooKeeper zk;
    private final ProfileZKReadClient reads;
    private final String root;
    private final List<ACL> acls;
    private final long timeoutNanos;

    public ZKProfileReservationStore(ZooKeeper zk, String root, List<ACL> acls, Duration timeout) {
        this.zk = Objects.requireNonNull(zk, "zk");
        reads = new ProfileZKReadClient(zk);
        PathUtils.validatePath(root);
        if (root.equals("/") || acls.isEmpty() || timeout.isNegative() || timeout.isZero()
                || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Invalid reservation namespace, ACLs or timeout");
        }
        this.root = root;
        this.acls = acls.stream().map(acl -> new ACL(acl.getPerms(),
                new Id(acl.getId().getScheme(), acl.getId().getId()))).toList();
        timeoutNanos = timeout.toNanos();
    }

    String path(long ledgerId) {
        if (ledgerId < 0) {
            throw new IllegalArgumentException("Invalid reservation ledger");
        }
        return root + "/" + Long.toHexString(ledgerId);
    }

    public ReadResult read(long ledgerId) {
        return read(ledgerId, System.nanoTime() + timeoutNanos);
    }

    private ReadResult read(long ledgerId, long deadline) {
        String path = path(ledgerId);
        try {
            if (reads.sync(root, deadline) != Code.OK.intValue()) {
                return new ReadResult(ReadStatus.UNAVAILABLE, null);
            }
            ProfileZKReadClient.Node node = reads.get(path, deadline);
            if (node.code() == Code.NONODE.intValue()) {
                return new ReadResult(ReadStatus.NOT_FOUND, null);
            }
            if (node.code() != Code.OK.intValue()) {
                return new ReadResult(ReadStatus.UNAVAILABLE, null);
            }
            Stat stat = node.stat();
            if (stat == null || stat.getEphemeralOwner() != 0 || stat.getVersion() < 0
                    || stat.getCzxid() < 0 || stat.getMzxid() < stat.getCzxid()) {
                return new ReadResult(ReadStatus.QUARANTINED, null);
            }
            ProfileReservation reservation = ProfileReservationCodec.decode(node.bytes());
            if (reservation.backlink().ledgerId() != ledgerId) {
                return new ReadResult(ReadStatus.QUARANTINED, null);
            }
            return new ReadResult(ReadStatus.FOUND, new Snapshot(stat.getMzxid(), reservation));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new ReadResult(ReadStatus.UNAVAILABLE, null);
        } catch (TimeoutException timeout) {
            return new ReadResult(ReadStatus.UNAVAILABLE, null);
        } catch (IllegalArgumentException invalid) {
            return new ReadResult(ReadStatus.QUARANTINED, null);
        }
    }

    public CreateResult createIfAbsent(ProfileReservation desired) {
        Objects.requireNonNull(desired, "desired");
        byte[] bytes = ProfileReservationCodec.encode(desired);
        long deadline = System.nanoTime() + timeoutNanos;
        boolean submitted = false;
        try {
            ProfileZKReadClient.requireTime(deadline);
            CompletableFuture<Integer> reply = new CompletableFuture<>();
            submitted = true;
            zk.create(path(desired.backlink().ledgerId()), bytes, acls, CreateMode.PERSISTENT,
                    (code, ignored, context, name) -> reply.complete(code), null);
            int code = ProfileZKReadClient.await(reply, deadline);
            // Always verify actual bytes and Stat, including after an acknowledged create.
            ReadResult current = read(desired.backlink().ledgerId(), deadline);
            if (current.status() == ReadStatus.QUARANTINED) {
                return new CreateResult(CreateStatus.QUARANTINED, null);
            }
            if (current.status() != ReadStatus.FOUND) {
                return new CreateResult(CreateStatus.DURABILITY_UNKNOWN, null);
            }
            if (!desired.equals(current.snapshot().reservation())) {
                return new CreateResult(CreateStatus.CONFLICT, null);
            }
            return new CreateResult(code == Code.OK.intValue() ? CreateStatus.APPLIED : CreateStatus.ALREADY_APPLIED,
                    current.snapshot());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return new CreateResult(submitted ? CreateStatus.DURABILITY_UNKNOWN : CreateStatus.UNAVAILABLE, null);
        } catch (TimeoutException timeout) {
            return new CreateResult(submitted ? CreateStatus.DURABILITY_UNKNOWN : CreateStatus.UNAVAILABLE, null);
        }
    }

    /** Bind the local target explicitly: AuthorityKey names the shared domain, not an arbitrary target or path. */
    public ProfileControlStore installAuthorityReader(String targetBookie) {
        ProfileAuthorityCodec.text(Objects.requireNonNull(targetBookie, "targetBookie"),
                ProfileControlScope.MAX_TARGET_BOOKIE_BYTES);
        return key -> {
            Objects.requireNonNull(key, "key");
            ReadResult current = read(key.ledgerId());
            if (current.status() != ReadStatus.FOUND) {
                return new ProfileControlStore.ReadResult(current.status(), null);
            }
            return current.snapshot().reservation().installs().stream()
                    .filter(scope -> scope.targetBookie().equals(targetBookie)
                            && ProfileAuthorityKey.fromScope(scope).equals(key))
                    .findFirst().map(scope -> ProfileControlStore.ReadResult.found(new CommittedProfileAuthority(
                            scope, CommittedProfileAuthority.State.PREPARING, EngineProfile.SEGMENT_WAL_ENGINE,
                            current.snapshot().storeVersion(), true)))
                    .orElseGet(ProfileControlStore.ReadResult::notFound);
        };
    }
}

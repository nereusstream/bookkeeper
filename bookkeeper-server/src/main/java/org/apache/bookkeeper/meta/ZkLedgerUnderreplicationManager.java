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

package org.apache.bookkeeper.meta;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.RateLimiter;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.CustomLog;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.DNS;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.CheckAllLedgersFormat;
import org.apache.bookkeeper.proto.LedgerRereplicationLayoutFormat;
import org.apache.bookkeeper.proto.LockDataFormat;
import org.apache.bookkeeper.proto.PlacementPolicyCheckFormat;
import org.apache.bookkeeper.proto.ReplicasCheckFormat;
import org.apache.bookkeeper.proto.UnderreplicatedLedgerFormat;
import org.apache.bookkeeper.replication.ReplicationEnableCb;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.SubTreeCache;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeper implementation of underreplication manager.
 * This is implemented in a hierarchical fashion, so it'll work with
 * FlatLedgerManagerFactory and HierarchicalLedgerManagerFactory.
 *
 * <p>Layout is:
 * /root/underreplication/ LAYOUT
 *                         ledgers/(hierarchicalpath)/urL(ledgerId)
 *                         locks/(ledgerId)
 *
 * <p>The hierarchical path is created by splitting the ledger into 4 2byte
 * segments which are represented in hexadecimal.
 * e.g. For ledger id 0xcafebeef0000feed, the path is
 *  cafe/beef/0000/feed/
 */
@CustomLog
public class ZkLedgerUnderreplicationManager implements LedgerUnderreplicationManager {
    static final String LAYOUT = "BASIC";
    static final int LAYOUT_VERSION = 1;

    private static final byte[] LOCK_DATA = getLockData();

    private static class Lock {
        private final String lockZNode;
        private final Optional<Integer> ledgerZNodeVersion;
        private final String profileGuard;
        private final long session;
        private final long markerBirth;
        private volatile long lockBirth;

        Lock(String lockZNode, Optional<Integer> ledgerZNodeVersion) {
            this(lockZNode, ledgerZNodeVersion, null, 0, 0);
        }

        Lock(String lockZNode, Optional<Integer> ledgerZNodeVersion, String profileGuard, long session,
                long markerBirth) {
            this.lockZNode = lockZNode;
            this.ledgerZNodeVersion = ledgerZNodeVersion;
            this.profileGuard = profileGuard;
            this.session = session;
            this.markerBirth = markerBirth;
        }

        String getLockZNode() {
            return lockZNode;
        }

        Optional<Integer> getLedgerZNodeVersion() {
            return ledgerZNodeVersion;
        }
    }
    private final Map<Long, Lock> heldLocks = new ConcurrentHashMap<Long, Lock>();
    private volatile boolean profileCleanupGuards;
    private boolean profileClosing;

    /** Opaque local receipt for one acquired task; a later lock for the same ledger is a different task. */
    public static final class ProfileTask {
        private final ZkLedgerUnderreplicationManager issuer;
        private final long ledger;
        private final Lock captured;

        private ProfileTask(ZkLedgerUnderreplicationManager issuer, long ledger, Lock captured) {
            this.issuer = issuer;
            this.ledger = ledger;
            this.captured = captured;
        }
    }

    public ProfileTask captureProfileTask(long ledger) {
        Lock captured = heldLocks.get(ledger);
        return captured == null || captured.profileGuard == null || captured.lockBirth == 0
                || captured.ledgerZNodeVersion.isEmpty() ? null : new ProfileTask(this, ledger, captured);
    }

    public void releaseProfileTask(ProfileTask task) throws UnavailableException {
        if (task != null && task.issuer == this) {
            releaseProfileLock(task.ledger, task.captured);
        }
    }
    private final Pattern idExtractionPattern;

    private final String rootPath;
    private final String basePath;
    private final String urLedgerPath;
    private final String urLockPath;
    private final String layoutZNode;
    private final AbstractConfiguration conf;
    private final String lostBookieRecoveryDelayZnode;
    private final String checkAllLedgersCtimeZnode;
    private final String placementPolicyCheckCtimeZnode;
    private final String replicasCheckCtimeZnode;
    private final ZooKeeper zkc;
    private final SubTreeCache subTreeCache;
    private final RateLimiter rateLimiter;

    /** Explicit isolated Profile opt-in, before acquiring any task. Default Classic behavior is unchanged. */
    public synchronized void enableProfileCleanupGuards() {
        if (!heldLocks.isEmpty() || profileClosing) {
            throw new IllegalStateException("Profile cleanup guards must be enabled before acquiring tasks");
        }
        profileCleanupGuards = true;
    }

    public boolean usesMutationClient(ZooKeeper client) {
        return zkc == client;
    }

    public ZkLedgerUnderreplicationManager(AbstractConfiguration conf, ZooKeeper zkc)
            throws UnavailableException, InterruptedException, ReplicationException.CompatibilityException {
        this.conf = conf;
        if (conf.getZkReplicationTaskRateLimit() > 0) {
            log.info()
                    .attr("permitsPerSecond", conf.getZkReplicationTaskRateLimit())
                    .log("Throttling acquire task rate is configured");
            rateLimiter = RateLimiter.create(conf.getZkReplicationTaskRateLimit());
        } else {
            log.info("Throttling acquire task rate is disabled");
            rateLimiter = null;
        }
        rootPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf);
        basePath = getBasePath(rootPath);
        layoutZNode = basePath + '/' + BookKeeperConstants.LAYOUT_ZNODE;
        urLedgerPath = basePath
                + BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH;
        urLockPath = basePath + '/' + BookKeeperConstants.UNDER_REPLICATION_LOCK;
        lostBookieRecoveryDelayZnode = basePath + '/' + BookKeeperConstants.LOSTBOOKIERECOVERYDELAY_NODE;
        checkAllLedgersCtimeZnode = basePath + '/' + BookKeeperConstants.CHECK_ALL_LEDGERS_CTIME;
        placementPolicyCheckCtimeZnode = basePath + '/' + BookKeeperConstants.PLACEMENT_POLICY_CHECK_CTIME;
        replicasCheckCtimeZnode = basePath + '/' + BookKeeperConstants.REPLICAS_CHECK_CTIME;
        idExtractionPattern = Pattern.compile("urL(\\d+)$");
        this.zkc = zkc;
        this.subTreeCache = new SubTreeCache(new SubTreeCache.TreeProvider() {
            @Override
            public List<String> getChildren(String path, Watcher watcher) throws InterruptedException, KeeperException {
                return zkc.getChildren(path, watcher);
            }
        });

        try {
            checkLayout();
        } catch (KeeperException ke) {
            throw ReplicationException.fromKeeperException("", ke);
        }
    }

    public static String getBasePath(String rootPath) {
        return String.format("%s/%s", rootPath, BookKeeperConstants.UNDER_REPLICATION_NODE);
    }

    public static String getUrLockPath(String rootPath) {
        return String.format("%s/%s", getBasePath(rootPath), BookKeeperConstants.UNDER_REPLICATION_LOCK);
    }

    public static byte[] getLockData() {
        LockDataFormat lockData = new LockDataFormat();
        try {
            lockData.setBookieId(DNS.getDefaultHost("default"));
        } catch (UnknownHostException uhe) {
            // if we cant get the address, ignore. it's optional
            // in the data structure in any case
        }
        return lockData.toTextFormat().getBytes(UTF_8);
    }

    private void checkLayout()
            throws KeeperException, InterruptedException, ReplicationException.CompatibilityException {
        List<ACL> zkAcls = ZkUtils.getACLs(conf);
        if (zkc.exists(basePath, false) == null) {
            try {
                zkc.create(basePath, new byte[0], zkAcls, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have created it
            }
        }
        while (true) {
            if (zkc.exists(layoutZNode, false) == null) {
                LedgerRereplicationLayoutFormat layout = new LedgerRereplicationLayoutFormat();
                layout.setType(LAYOUT).setVersion(LAYOUT_VERSION);
                try {
                    zkc.create(layoutZNode, layout.toTextFormat().getBytes(UTF_8),
                               zkAcls, CreateMode.PERSISTENT);
                    break;
                } catch (KeeperException.NodeExistsException nne) {
                    // someone else managed to create it
                }
            } else {
                byte[] layoutData = zkc.getData(layoutZNode, false, null);

                LedgerRereplicationLayoutFormat layout = new LedgerRereplicationLayoutFormat();

                try {
                    layout.parseFromTextFormat(layoutData);
                    if (!layout.getType().equals(LAYOUT)
                            || layout.getVersion() != LAYOUT_VERSION) {
                        throw new ReplicationException.CompatibilityException(
                                "Incompatible layout found (" + LAYOUT + ":" + LAYOUT_VERSION + ")");
                    }
                } catch (RuntimeException pe) {
                    throw new ReplicationException.CompatibilityException(
                            "Invalid data found", pe);
                }
                break;
            }
        }
        if (zkc.exists(urLedgerPath, false) == null) {
            try {
                zkc.create(urLedgerPath, new byte[0], zkAcls, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have created it
            }
        }
        if (zkc.exists(urLockPath, false) == null) {
            try {
                zkc.create(urLockPath, new byte[0], zkAcls, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have created it
            }
        }
    }

    private long getLedgerId(String path) throws NumberFormatException {
        Matcher m = idExtractionPattern.matcher(path);
        if (m.find()) {
            return Long.parseLong(m.group(1));
        } else {
            throw new NumberFormatException("Couldn't find ledgerid in path");
        }
    }

    public static String getParentZnodePath(String base, long ledgerId) {
        String subdir1 = String.format("%04x", ledgerId >> 48 & 0xffff);
        String subdir2 = String.format("%04x", ledgerId >> 32 & 0xffff);
        String subdir3 = String.format("%04x", ledgerId >> 16 & 0xffff);
        String subdir4 = String.format("%04x", ledgerId & 0xffff);

        return String.format("%s/%s/%s/%s/%s",
                             base, subdir1, subdir2, subdir3, subdir4);
    }

    public static String getUrLedgerZnode(String base, long ledgerId) {
        return String.format("%s/urL%010d", getParentZnodePath(base, ledgerId), ledgerId);
    }

    public static String getUrLedgerLockZnode(String base, long ledgerId) {
        return String.format("%s/urL%010d", base, ledgerId);
    }

    private String getUrLedgerZnode(long ledgerId) {
        return getUrLedgerZnode(urLedgerPath, ledgerId);
    }

    @Override
    public UnderreplicatedLedger getLedgerUnreplicationInfo(long ledgerId)
            throws ReplicationException.UnavailableException {
        try {
            String znode = getUrLedgerZnode(ledgerId);
            UnderreplicatedLedgerFormat fmt = new UnderreplicatedLedgerFormat();
            byte[] data = null;
            try {
                data = zkc.getData(znode, false, null);
            } catch (KeeperException.NoNodeException nne) {
                log.debug().attr("ledgerId", ledgerId).log("Ledger is not marked underreplicated");
                return null;
            }
            fmt.parseFromTextFormat(data);
            UnderreplicatedLedger underreplicatedLedger = new UnderreplicatedLedger(ledgerId);
            List<String> replicaList = fmt.getReplicasList();
            long ctime = (fmt.hasCtime() ? fmt.getCtime()
                    : UnderreplicatedLedger.UNASSIGNED_CTIME);
            underreplicatedLedger.setCtime(ctime);
            underreplicatedLedger.setReplicaList(replicaList);
            return underreplicatedLedger;
        } catch (KeeperException ke) {
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while connecting zookeeper", ie);
        } catch (RuntimeException pe) {
            throw new ReplicationException.UnavailableException("Error parsing proto message", pe);
        }
    }

    @Override
    public CompletableFuture<Void> markLedgerUnderreplicatedAsync(long ledgerId, Collection<String> missingReplicas) {
        log.debug()
                .attr("ledgerId", ledgerId)
                .attr("missingReplicas", missingReplicas)
                .log("markLedgerUnderreplicated");
        final List<ACL> zkAcls = ZkUtils.getACLs(conf);
        final String znode = getUrLedgerZnode(ledgerId);
        final CompletableFuture<Void> createFuture = new CompletableFuture<>();
        tryMarkLedgerUnderreplicatedAsync(znode, missingReplicas, zkAcls, createFuture);
        return createFuture;
    }

    private void tryMarkLedgerUnderreplicatedAsync(final String znode,
                                                   final Collection<String> missingReplicas,
                                                   final List<ACL> zkAcls,
                                                   final CompletableFuture<Void> finalFuture) {
        final UnderreplicatedLedgerFormat fmt = new UnderreplicatedLedgerFormat();
        if (conf.getStoreSystemTimeAsLedgerUnderreplicatedMarkTime()) {
            fmt.setCtime(System.currentTimeMillis());
        }
        missingReplicas.forEach(fmt::addReplica);
        final byte[] urLedgerData = fmt.toTextFormat().getBytes(UTF_8);
        ZkUtils.asyncCreateFullPathOptimistic(
            zkc, znode, urLedgerData, zkAcls, CreateMode.PERSISTENT,
            (rc, path, ctx, name) -> {
                if (Code.OK.intValue() == rc) {
                    FutureUtils.complete(finalFuture, null);
                } else if (Code.NODEEXISTS.intValue() == rc) {
                    // we need to handle the case where the ledger has been marked as underreplicated
                    handleLedgerUnderreplicatedAlreadyMarked(znode, missingReplicas, zkAcls, finalFuture);
                } else {
                    FutureUtils.completeExceptionally(finalFuture, KeeperException.create(Code.get(rc)));
                }
            }, null);
    }


    private void handleLedgerUnderreplicatedAlreadyMarked(final String znode,
                                                          final Collection<String> missingReplicas,
                                                          final List<ACL> zkAcls,
                                                          final CompletableFuture<Void> finalFuture) {
        // get the existing underreplicated ledger data
        zkc.getData(znode, false, (getRc, getPath, getCtx, existingUrLedgerData, getStat) -> {
            if (Code.OK.intValue() == getRc) {
                // deserialize existing underreplicated ledger data
                final UnderreplicatedLedgerFormat fmt = new UnderreplicatedLedgerFormat();
                try {
                    fmt.parseFromTextFormat(existingUrLedgerData);
                } catch (RuntimeException e) {
                    // corrupted metadata in zookeeper
                    FutureUtils.completeExceptionally(finalFuture,
                        new ReplicationException.UnavailableException(
                            "Invalid underreplicated ledger data for ledger " + znode, e));
                    return;
                }
                boolean replicaAdded = false;
                for (String missingReplica : missingReplicas) {
                    if (fmt.getReplicasList().contains(missingReplica)) {
                        continue;
                    } else {
                        fmt.addReplica(missingReplica);
                        replicaAdded = true;
                    }
                }
                if (!replicaAdded) { // no new missing replica is added
                    FutureUtils.complete(finalFuture, null);
                    return;
                }
                if (conf.getStoreSystemTimeAsLedgerUnderreplicatedMarkTime()) {
                    fmt.setCtime(System.currentTimeMillis());
                }
                final byte[] newUrLedgerData = fmt.toTextFormat().getBytes(UTF_8);
                zkc.setData(znode, newUrLedgerData, getStat.getVersion(), (setRc, setPath, setCtx, setStat) -> {
                    if (Code.OK.intValue() == setRc) {
                        FutureUtils.complete(finalFuture, null);
                    } else if (Code.NONODE.intValue() == setRc) {
                        tryMarkLedgerUnderreplicatedAsync(znode, missingReplicas, zkAcls, finalFuture);
                    } else if (Code.BADVERSION.intValue() == setRc) {
                        handleLedgerUnderreplicatedAlreadyMarked(znode, missingReplicas, zkAcls, finalFuture);
                    } else {
                        FutureUtils.completeExceptionally(finalFuture, KeeperException.create(Code.get(setRc)));
                    }
                }, null);
            } else if (Code.NONODE.intValue() == getRc) {
                tryMarkLedgerUnderreplicatedAsync(znode, missingReplicas, zkAcls, finalFuture);
            } else {
                FutureUtils.completeExceptionally(finalFuture, KeeperException.create(Code.get(getRc)));
            }
        }, null);
    }

    @Override
    public void markLedgerReplicated(long ledgerId) throws ReplicationException.UnavailableException {
        log.debug().attr("ledgerId", ledgerId).log("markLedgerReplicated");
        Lock captured = heldLocks.get(ledgerId);
        if (captured != null && captured.profileGuard != null) {
            try {
                markGuardedLedgerReplicated(ledgerId, captured, List.of());
            } finally {
                releaseUnderreplicatedLedger(ledgerId);
            }
            return;
        }
        try {
            Lock l = heldLocks.get(ledgerId);
            if (l != null) {
                final Optional<Integer> ledgerZNodeVersion = l.getLedgerZNodeVersion();
                if (ledgerZNodeVersion.isPresent()) {
                    zkc.delete(getUrLedgerZnode(ledgerId), ledgerZNodeVersion.get());

                    try {
                        // clean up the hierarchy
                        String[] parts = getUrLedgerZnode(ledgerId).split("/");
                        for (int i = 1; i <= 4; i++) {
                            String[] p = Arrays.copyOf(parts, parts.length - i);
                            String path = Joiner.on("/").join(p);
                            Stat s = zkc.exists(path, null);
                            if (s != null) {
                                zkc.delete(path, s.getVersion());
                            }
                        }
                    } catch (KeeperException.NotEmptyException nee) {
                        // This can happen when cleaning up the hierarchy.
                        // It's safe to ignore, it simply means another
                        // ledger in the same hierarchy has been marked as
                        // underreplicated.
                    }
                }
            }
        } catch (KeeperException.NoNodeException nne) {
            // this is ok
        } catch (KeeperException.BadVersionException bve) {
            // if this is the case, some has marked the ledger
            // for rereplication again. Leave the underreplicated
            // znode in place, so the ledger is checked.
        } catch (KeeperException ke) {
            log.error().exception(ke).log("Error deleting underreplicated ledger znode");
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        } finally {
            releaseUnderreplicatedLedger(ledgerId);
        }
    }

    /** Consume checks produced by the Profile cold authority path in the same marker/lock removal transaction.
     * A false result leaves the task pending; unknown I/O throws and retains the local lock obligation.
     * Direct administrative lock reset/recreation is outside this opt-in protocol.
     */
    public boolean markProfileLedgerReplicated(ProfileTask task, List<Op> authorityChecks) throws UnavailableException {
        var checks = List.copyOf(authorityChecks);
        if (checks.isEmpty() || checks.size() > 256 || checks.stream().anyMatch(check ->
                check.getType() != ZooDefs.OpCode.check
                    || ((org.apache.zookeeper.proto.CheckVersionRequest) check.toRequestRecord()).getVersion() < 0)) {
            throw new IllegalArgumentException("Profile completion requires bounded exact-version authority checks");
        }
        return task != null && task.issuer == this
                && markGuardedLedgerReplicated(task.ledger, task.captured, checks);
    }

    private synchronized void acquireProfileLock(long ledgerId, String lockPath, Optional<Integer> version,
            long markerBirth) throws KeeperException, InterruptedException {
        if (profileClosing) {
            throw new IllegalStateException("Profile replication manager is closing");
        }
        if (heldLocks.containsKey(ledgerId)) {
            throw new KeeperException.NodeExistsException(lockPath);
        }
        var captured = new Lock(lockPath, version, lockPath + "-profile-" + UUID.randomUUID(),
                zkc.getSessionId(), markerBirth);
        // Serialize release with the actual acquire, including an unknown create reply.
        synchronized (captured) {
            heldLocks.put(ledgerId, captured);
            try {
                zkc.multi(List.of(Op.create(lockPath, LOCK_DATA, ZkUtils.getACLs(conf), CreateMode.EPHEMERAL),
                        Op.create(captured.profileGuard, profileLockBytes(captured), immutableProfileAcls(),
                                CreateMode.EPHEMERAL)));
                if (!ownsProfileLock(captured)) {
                    throw KeeperException.create(Code.SESSIONEXPIRED, lockPath);
                }
                var created = zkc.exists(lockPath, false);
                if (created == null || created.getEphemeralOwner() != captured.session) {
                    throw KeeperException.create(Code.SESSIONEXPIRED, lockPath);
                }
                captured.lockBirth = created.getCzxid();
            } catch (KeeperException.NodeExistsException occupied) {
                heldLocks.remove(ledgerId, captured);
                throw occupied;
            }
        }
    }

    private List<ACL> immutableProfileAcls() {
        var acls = ZkUtils.getACLs(conf).stream().filter(acl -> (acl.getPerms() & ZooDefs.Perms.READ) != 0)
                .map(acl -> new ACL(ZooDefs.Perms.READ, acl.getId())).toList();
        if (acls.isEmpty()) {
            throw new IllegalArgumentException("Profile replication witnesses require readable ACLs");
        }
        return acls;
    }

    private static byte[] profileLockBytes(Lock captured) {
        return (captured.lockZNode + ":" + Long.toHexString(captured.session)).getBytes(UTF_8);
    }

    private boolean ownsProfileLock(Lock captured) throws KeeperException, InterruptedException {
        if (!zkc.getState().isAlive() || zkc.getSessionId() != captured.session) {
            return false;
        }
        var guard = new Stat();
        byte[] witness;
        try {
            witness = zkc.getData(captured.profileGuard, false, guard);
        } catch (KeeperException.NoNodeException absent) {
            return false;
        }
        var lock = zkc.exists(captured.lockZNode, false);
        return lock != null && lock.getVersion() == 0 && lock.getEphemeralOwner() == captured.session
                && (captured.lockBirth == 0 || lock.getCzxid() == captured.lockBirth)
                && guard.getVersion() == 0 && guard.getEphemeralOwner() == captured.session
                && Arrays.equals(witness, profileLockBytes(captured));
    }

    private boolean markGuardedLedgerReplicated(long ledgerId, Lock captured, List<Op> checks)
            throws UnavailableException {
        synchronized (captured) {
            try {
                if (heldLocks.get(ledgerId) != captured || captured.ledgerZNodeVersion.isEmpty()
                        || captured.lockBirth == 0 || !ownsProfileLock(captured)) {
                    return false;
                }
                String marker = getUrLedgerZnode(ledgerId);
                var before = zkc.exists(marker, false);
                if (before == null || before.getCzxid() != captured.markerBirth
                        || before.getVersion() != captured.ledgerZNodeVersion.get()) {
                    return false;
                }
                String birth = marker + "/profile-birth-" + Long.toHexString(captured.markerBirth);
                byte[] expected = java.nio.ByteBuffer.allocate(8).putLong(captured.markerBirth).array();
                try {
                    zkc.multi(List.of(Op.check(captured.profileGuard, 0), Op.check(captured.lockZNode, 0),
                            Op.check(marker, captured.ledgerZNodeVersion.get()),
                            Op.create(birth, expected, immutableProfileAcls(), CreateMode.PERSISTENT)));
                } catch (KeeperException.NodeExistsException retained) {
                    // The immutable physical-birth witness is shared by retries of this marker only.
                }
                var identity = new Stat();
                byte[] actual = zkc.getData(birth, false, identity);
                var after = zkc.exists(marker, false);
                if (after != null && after.getCzxid() != captured.markerBirth
                        && identity.getVersion() == 0 && identity.getEphemeralOwner() == 0
                        && Arrays.equals(actual, expected)) {
                    // A replacement between the pre-read and witness creation must not retain our stale child.
                    zkc.multi(List.of(Op.check(marker, after.getVersion()), Op.check(birth, 0), Op.delete(birth, 0)));
                    return false;
                }
                if (after == null || after.getCzxid() != captured.markerBirth
                        || after.getVersion() != captured.ledgerZNodeVersion.get()
                        || identity.getVersion() != 0 || identity.getEphemeralOwner() != 0
                        || !Arrays.equals(actual, expected) || !ownsProfileLock(captured)) {
                    return false;
                }
                var operations = new java.util.ArrayList<>(checks);
                operations.add(Op.check(captured.profileGuard, 0));
                operations.add(Op.check(captured.lockZNode, 0));
                operations.add(Op.check(birth, 0));
                operations.add(Op.check(marker, captured.ledgerZNodeVersion.get()));
                operations.add(Op.delete(birth, 0));
                operations.add(Op.delete(marker, captured.ledgerZNodeVersion.get()));
                operations.add(Op.delete(captured.lockZNode, 0));
                operations.add(Op.delete(captured.profileGuard, 0));
                zkc.multi(operations);
                heldLocks.remove(ledgerId, captured);
                return true;
            } catch (KeeperException.NoNodeException | KeeperException.BadVersionException
                    | KeeperException.NotEmptyException changed) {
                return false;
            } catch (KeeperException unavailable) {
                throw ReplicationException.fromKeeperException("Profile marker cleanup outcome unknown", unavailable);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new UnavailableException("Interrupted during Profile marker cleanup", interrupted);
            }
        }
    }

    private void releaseProfileLock(long ledgerId, Lock captured) throws UnavailableException {
        synchronized (captured) {
            try {
                if (heldLocks.get(ledgerId) == captured && ownsProfileLock(captured)) {
                    zkc.multi(List.of(Op.check(captured.profileGuard, 0), Op.check(captured.lockZNode, 0),
                            Op.delete(captured.lockZNode, 0), Op.delete(captured.profileGuard, 0)));
                }
                heldLocks.remove(ledgerId, captured);
            } catch (KeeperException.NoNodeException | KeeperException.BadVersionException lost) {
                heldLocks.remove(ledgerId, captured);
            } catch (KeeperException unavailable) {
                throw ReplicationException.fromKeeperException("Profile lock release outcome unknown", unavailable);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new UnavailableException("Interrupted during Profile lock release", interrupted);
            }
        }
    }

    /**
     * Get a list of all the underreplicated ledgers which have been
     * marked for rereplication, filtered by the predicate on the replicas list.
     *
     * <p>Replicas list of an underreplicated ledger is the list of the bookies which are part of
     * the ensemble of this ledger and are currently unavailable/down.
     *
     * @param predicate filter to use while listing under replicated ledgers. 'null' if filtering is not required.
     * @return an iterator which returns underreplicated ledgers.
     */
    @Override
    public Iterator<UnderreplicatedLedger> listLedgersToRereplicate(final Predicate<List<String>> predicate) {
        final Queue<String> queue = new LinkedList<String>();
        queue.add(urLedgerPath);

        return new Iterator<UnderreplicatedLedger>() {
            final Queue<UnderreplicatedLedger> curBatch = new LinkedList<UnderreplicatedLedger>();

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                if (curBatch.size() > 0) {
                    return true;
                }

                while (queue.size() > 0 && curBatch.size() == 0) {
                    String parent = queue.remove();
                    try {
                        for (String c : zkc.getChildren(parent, false)) {
                            String child = parent + "/" + c;
                            if (c.startsWith("urL")) {
                                long ledgerId = getLedgerId(child);
                                UnderreplicatedLedger underreplicatedLedger = getLedgerUnreplicationInfo(ledgerId);
                                if (underreplicatedLedger != null) {
                                    List<String> replicaList = underreplicatedLedger.getReplicaList();
                                    if ((predicate == null) || predicate.test(replicaList)) {
                                        curBatch.add(underreplicatedLedger);
                                    }
                                }
                            } else {
                                queue.add(child);
                            }
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    } catch (KeeperException.NoNodeException nne) {
                        // ignore
                    } catch (Exception e) {
                        throw new RuntimeException("Error reading list", e);
                    }
                }
                return curBatch.size() > 0;
            }

            @Override
            public UnderreplicatedLedger next() {
                assert curBatch.size() > 0;
                return curBatch.remove();
            }
        };
    }

    private long getLedgerToRereplicateFromHierarchy(String parent, long depth)
            throws KeeperException, InterruptedException {
        if (depth == 4) {
            List<String> children;
            try {
                children = subTreeCache.getChildren(parent);
            } catch (KeeperException.NoNodeException nne) {
                // can occur if another underreplicated ledger's
                // hierarchy is being cleaned up
                return -1;
            }

            Collections.shuffle(children);
            List<ACL> zkAcls = ZkUtils.getACLs(conf);
            while (children.size() > 0) {
                String tryChild = children.get(0);
                try {
                    List<String> locks = subTreeCache.getChildren(urLockPath);
                    if (locks.contains(tryChild)) {
                        children.remove(tryChild);
                        continue;
                    }

                    Stat stat = zkc.exists(parent + "/" + tryChild, false);
                    if (stat == null) {
                        log.debug()
                                .attr("parent", parent)
                                .attr("child", tryChild)
                                .log("Node doesn't exist");
                        children.remove(tryChild);
                        continue;
                    }

                    String lockPath = urLockPath + "/" + tryChild;
                    long ledgerId = getLedgerId(tryChild);
                    if (profileCleanupGuards) {
                        acquireProfileLock(ledgerId, lockPath, Optional.of(stat.getVersion()), stat.getCzxid());
                    } else {
                        zkc.create(lockPath, LOCK_DATA, zkAcls, CreateMode.EPHEMERAL);
                        heldLocks.put(ledgerId, new Lock(lockPath, Optional.of(stat.getVersion())));
                    }
                    return ledgerId;
                } catch (KeeperException.NodeExistsException nee) {
                    children.remove(tryChild);
                } catch (NumberFormatException nfe) {
                    children.remove(tryChild);
                }
            }
            return -1;
        }

        List<String> children;
        try {
            children = subTreeCache.getChildren(parent);
        } catch (KeeperException.NoNodeException nne) {
            // can occur if another underreplicated ledger's
            // hierarchy is being cleaned up
            return -1;
        }

        Collections.shuffle(children);

        while (children.size() > 0) {
            String tryChild = children.get(0);
            String tryPath = parent + "/" + tryChild;
            long ledger = getLedgerToRereplicateFromHierarchy(tryPath, depth + 1);
            if (ledger != -1) {
                return ledger;
            }
            children.remove(tryChild);
        }
        return -1;
    }


    @Override
    public long pollLedgerToRereplicate() throws ReplicationException.UnavailableException {
        log.debug("pollLedgerToRereplicate()");
        try {
            return getLedgerToRereplicateFromHierarchy(urLedgerPath, 0);
        } catch (KeeperException ke) {
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while connecting zookeeper", ie);
        }
    }

    @Override
    public long getLedgerToRereplicate() throws ReplicationException.UnavailableException {
        log.debug("getLedgerToRereplicate()");
        while (true) {
            if (rateLimiter != null) {
                rateLimiter.acquire();
            }
            final CountDownLatch changedLatch = new CountDownLatch(1);
            Watcher w = new Watcher() {
                @Override
                public void process(WatchedEvent e) {
                    log.info().attr("event", e).log("Latch countdown due to ZK event");
                    changedLatch.countDown();
                }
            };
            try (SubTreeCache.WatchGuard wg = subTreeCache.registerWatcherWithGuard(w)) {
                waitIfLedgerReplicationDisabled();
                long ledger = getLedgerToRereplicateFromHierarchy(urLedgerPath, 0);
                if (ledger != -1) {
                    return ledger;
                }
                // nothing found, wait for a watcher to trigger
                changedLatch.await();
            } catch (KeeperException ke) {
                throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new ReplicationException.UnavailableException("Interrupted while connecting zookeeper", ie);
            }
        }
    }

    private void waitIfLedgerReplicationDisabled() throws UnavailableException,
            InterruptedException {
        if (!this.isLedgerReplicationEnabled()) {
            ReplicationEnableCb cb = new ReplicationEnableCb();
            this.notifyLedgerReplicationEnabled(cb);
            cb.await();
        }
    }

    @Override
    public void releaseUnderreplicatedLedger(long ledgerId) throws ReplicationException.UnavailableException {
        log.debug().attr("ledgerId", ledgerId).log("releaseLedger");
        Lock captured = heldLocks.get(ledgerId);
        if (captured != null && captured.profileGuard != null) {
            releaseProfileLock(ledgerId, captured);
            return;
        }
        try {
            Lock l = heldLocks.get(ledgerId);
            if (l != null) {
                zkc.delete(l.getLockZNode(), -1);
            }
        } catch (KeeperException.NoNodeException nne) {
            // this is ok
        } catch (KeeperException ke) {
            log.error().exception(ke).log("Error deleting underreplicated ledger lock");
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while connecting zookeeper", ie);
        }
        heldLocks.remove(ledgerId);
    }

    @Override
    public void close() throws ReplicationException.UnavailableException {
        log.debug("close()");
        if (profileCleanupGuards) {
            synchronized (this) {
                profileClosing = true;
            }
            for (var entry : heldLocks.entrySet()) {
                releaseProfileLock(entry.getKey(), entry.getValue());
            }
            return;
        }
        try {
            for (Map.Entry<Long, Lock> e : heldLocks.entrySet()) {
                zkc.delete(e.getValue().getLockZNode(), -1);
            }
        } catch (KeeperException.NoNodeException nne) {
            // this is ok
        } catch (KeeperException ke) {
            log.error().exception(ke).log("Error deleting underreplicated ledger lock");
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while connecting zookeeper", ie);
        }
    }

    @Override
    public void disableLedgerReplication()
            throws ReplicationException.UnavailableException {
        List<ACL> zkAcls = ZkUtils.getACLs(conf);
        log.debug("disableLedegerReplication()");
        try {
            String znode = basePath + '/' + BookKeeperConstants.DISABLE_NODE;
            zkc.create(znode, "".getBytes(UTF_8), zkAcls, CreateMode.PERSISTENT);
            log.info("Auto ledger re-replication is disabled!");
        } catch (KeeperException.NodeExistsException ke) {
            log.warn().exception(ke).log("AutoRecovery is already disabled!");
            throw new ReplicationException.UnavailableException(
                    "AutoRecovery is already disabled!", ke);
        } catch (KeeperException ke) {
            log.error().exception(ke).log("Exception while stopping auto ledger re-replication");
            throw ReplicationException.fromKeeperException("Exception while stopping auto ledger re-replication", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException(
                    "Interrupted while stopping auto ledger re-replication", ie);
        }
    }

    @Override
    public void enableLedgerReplication()
            throws ReplicationException.UnavailableException {
        log.debug("enableLedegerReplication()");
        try {
            zkc.delete(basePath + '/' + BookKeeperConstants.DISABLE_NODE, -1);
            log.info("Resuming automatic ledger re-replication");
        } catch (KeeperException.NoNodeException ke) {
            log.warn().exception(ke).log("AutoRecovery is already enabled!");
            throw new ReplicationException.UnavailableException(
                    "AutoRecovery is already enabled!", ke);
        } catch (KeeperException ke) {
            log.error().exception(ke).log("Exception while resuming ledger replication");
            throw ReplicationException.fromKeeperException("Exception while resuming auto ledger re-replication", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException(
                    "Interrupted while resuming auto ledger re-replication", ie);
        }
    }

    @Override
    public boolean isLedgerReplicationEnabled()
            throws ReplicationException.UnavailableException {
        log.debug("isLedgerReplicationEnabled()");
        try {
            return null == zkc.exists(basePath + '/'
                + BookKeeperConstants.DISABLE_NODE, false);
        } catch (KeeperException ke) {
            log.error().exception(ke).log("Error while checking the state of ledger re-replication");
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException(
                    "Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public void notifyLedgerReplicationEnabled(final GenericCallback<Void> cb)
            throws ReplicationException.UnavailableException {
        log.debug("notifyLedgerReplicationEnabled()");
        Watcher w = new Watcher() {
            @Override
            public void process(WatchedEvent e) {
                if (e.getType() == Watcher.Event.EventType.NodeDeleted) {
                    log.info("LedgerReplication is enabled externally through Zookeeper, "
                            + "since DISABLE_NODE ZNode is deleted");
                    cb.operationComplete(0, null);
                }
            }
        };
        try {
            if (null == zkc.exists(basePath + '/'
                    + BookKeeperConstants.DISABLE_NODE, w)) {
                log.info("LedgerReplication is enabled externally through Zookeeper, "
                        + "since DISABLE_NODE ZNode is deleted");
                cb.operationComplete(0, null);
                return;
            }
        } catch (KeeperException ke) {
            log.error().exception(ke).log("Error while checking the state of ledger re-replication");
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException(
                    "Interrupted while contacting zookeeper", ie);
        }
    }

    /**
     * Check whether the ledger is being replicated by any bookie.
     */
    @Override
    public boolean isLedgerBeingReplicated(long ledgerId) throws ReplicationException {
        try {
            return zkc.exists(getUrLedgerLockZnode(urLockPath, ledgerId), false) != null;
        } catch (Exception e) {
            throw new ReplicationException.UnavailableException("Failed to check ledger lock", e);
        }
    }

    /**
     * Acquire the underreplicated ledger lock.
     */
    public static String acquireUnderreplicatedLedgerLock(ZooKeeper zkc, String zkLedgersRootPath,
        long ledgerId, List<ACL> zkAcls)
            throws UnavailableException, InterruptedException {
        try {
            final String lockPath = getUrLedgerLockZnode(getUrLockPath(zkLedgersRootPath), ledgerId);
            ZkUtils.createFullPathOptimistic(zkc, lockPath, LOCK_DATA, zkAcls, CreateMode.EPHEMERAL);
            return lockPath;
        } catch (KeeperException ke) {
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        }
    }

    @Override
    public void acquireUnderreplicatedLedger(long ledgerId)
            throws ReplicationException  {
        try {
            if (profileCleanupGuards) {
                acquireProfileLock(ledgerId, getUrLedgerLockZnode(urLockPath, ledgerId), Optional.empty(), 0);
                return;
            }
            final String lockPath = acquireUnderreplicatedLedgerLock(zkc, rootPath, ledgerId, ZkUtils.getACLs(conf));
            heldLocks.put(ledgerId, new Lock(lockPath, Optional.empty()));
        } catch (Exception e) {
            throw new ReplicationException.UnavailableException(
                    "Failed to acquire underreplicated ledger lock for " + ledgerId, e);
        }
    }

    @Override
    public boolean initializeLostBookieRecoveryDelay(int lostBookieRecoveryDelay) throws UnavailableException {
        log.debug("initializeLostBookieRecoveryDelay()");
        try {
            zkc.create(lostBookieRecoveryDelayZnode, Integer.toString(lostBookieRecoveryDelay).getBytes(UTF_8),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ke) {
            log.info("lostBookieRecoveryDelay Znode is already present, so using "
                    + "existing lostBookieRecoveryDelay Znode value");
            return false;
        } catch (KeeperException.NoNodeException nne) {
            log.error()
                    .exception(nne)
                    .log("lostBookieRecoveryDelay Znode not found. Please verify if Auditor has been initialized.");
            return false;
        } catch (KeeperException ke) {
            log.error().exception(ke).log("Error while initializing LostBookieRecoveryDelay");
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
        return true;
    }

    @Override
    public void setLostBookieRecoveryDelay(int lostBookieRecoveryDelay) throws UnavailableException {
        log.debug("setLostBookieRecoveryDelay()");
        try {
            if (zkc.exists(lostBookieRecoveryDelayZnode, false) != null) {
                zkc.setData(lostBookieRecoveryDelayZnode, Integer.toString(lostBookieRecoveryDelay).getBytes(UTF_8),
                        -1);
            } else {
                zkc.create(lostBookieRecoveryDelayZnode, Integer.toString(lostBookieRecoveryDelay).getBytes(UTF_8),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException ke) {
            log.error().exception(ke).log("Error while setting LostBookieRecoveryDelay");
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public int getLostBookieRecoveryDelay() throws UnavailableException {
        log.debug("getLostBookieRecoveryDelay()");
        try {
            byte[] data = zkc.getData(lostBookieRecoveryDelayZnode, false, null);
            return Integer.parseInt(new String(data, UTF_8));
        } catch (KeeperException ke) {
            log.error().exception(ke).log("Error while getting LostBookieRecoveryDelay");
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public void notifyUnderReplicationLedgerChanged(GenericCallback<Void> cb) throws UnavailableException {
        log.debug("notifyUnderReplicationLedgerChanged()");
        Watcher w = new Watcher() {
            @Override
            public void process(WatchedEvent e) {
                if (e.getType() == Event.EventType.NodeDeleted && idExtractionPattern.matcher(e.getPath()).find()) {
                    cb.operationComplete(0, null);
                }
            }
        };
        try {
            zkc.addWatch(urLedgerPath, w, AddWatchMode.PERSISTENT_RECURSIVE);
        } catch (KeeperException ke) {
            log.error().exception(ke).log("Error while checking the state of underReplicated ledgers");
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public void notifyLostBookieRecoveryDelayChanged(GenericCallback<Void> cb) throws UnavailableException {
        log.debug("notifyLostBookieRecoveryDelayChanged()");
        Watcher w = new Watcher() {
            @Override
            public void process(WatchedEvent e) {
                if (e.getType() == Watcher.Event.EventType.NodeDataChanged) {
                    cb.operationComplete(0, null);
                }
            }
        };
        try {
            if (null == zkc.exists(lostBookieRecoveryDelayZnode, w)) {
                cb.operationComplete(0, null);
                return;
            }
        } catch (KeeperException ke) {
            log.error().exception(ke).log("Error while checking the state of lostBookieRecoveryDelay");
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public String getReplicationWorkerIdRereplicatingLedger(long ledgerId)
            throws ReplicationException.UnavailableException {
        String replicationWorkerId = null;
        try {
            byte[] lockData = zkc.getData(getUrLedgerLockZnode(urLockPath, ledgerId), false, null);
            LockDataFormat lock = new LockDataFormat();
            lock.parseFromTextFormat(lockData);
            replicationWorkerId = lock.getBookieId();
        } catch (KeeperException.NoNodeException e) {
            // this is ok.
        } catch (KeeperException e) {
            log.error().exception(e).log("Error while getting ReplicationWorkerId rereplicating Ledger");
            throw ReplicationException.fromKeeperException(
                    "Error while getting ReplicationWorkerId rereplicating Ledger", e);
        } catch (InterruptedException e) {
            log.error().exception(e).log("Got interrupted while getting ReplicationWorkerId rereplicating Ledger");
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", e);
        } catch (RuntimeException e) {
            log.error().exception(e).log("Error while parsing ZK data of lock");
            throw new ReplicationException.UnavailableException("Error while parsing ZK data of lock", e);
        }
        return replicationWorkerId;
    }

    @Override
    public void setCheckAllLedgersCTime(long checkAllLedgersCTime) throws UnavailableException {
        log.debug("setCheckAllLedgersCTime");
        try {
            List<ACL> zkAcls = ZkUtils.getACLs(conf);
            CheckAllLedgersFormat fmt = new CheckAllLedgersFormat();
            fmt.setCheckAllLedgersCTime(checkAllLedgersCTime);
            byte[] checkAllLedgersFormatByteArray = fmt.toByteArray();
            if (zkc.exists(checkAllLedgersCtimeZnode, false) != null) {
                zkc.setData(checkAllLedgersCtimeZnode, checkAllLedgersFormatByteArray, -1);
            } else {
                zkc.create(checkAllLedgersCtimeZnode, checkAllLedgersFormatByteArray, zkAcls, CreateMode.PERSISTENT);
            }
        } catch (KeeperException ke) {
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public long getCheckAllLedgersCTime() throws UnavailableException {
        log.debug("getCheckAllLedgersCTime");
        try {
            byte[] data = zkc.getData(checkAllLedgersCtimeZnode, false, null);
            CheckAllLedgersFormat checkAllLedgersFormat = new CheckAllLedgersFormat();
            checkAllLedgersFormat.parseFrom(data);
            return checkAllLedgersFormat.hasCheckAllLedgersCTime() ? checkAllLedgersFormat.getCheckAllLedgersCTime()
                    : -1;
        } catch (KeeperException.NoNodeException ne) {
            log.warn("checkAllLedgersCtimeZnode is not yet available");
            return -1;
        } catch (KeeperException ke) {
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        } catch (RuntimeException ipbe) {
            throw new ReplicationException.UnavailableException("Error while parsing ZK protobuf binary data", ipbe);
        }
    }

    @Override
    public void setPlacementPolicyCheckCTime(long placementPolicyCheckCTime) throws UnavailableException {
        log.debug("setPlacementPolicyCheckCTime");
        try {
            List<ACL> zkAcls = ZkUtils.getACLs(conf);
            PlacementPolicyCheckFormat fmt = new PlacementPolicyCheckFormat();
            fmt.setPlacementPolicyCheckCTime(placementPolicyCheckCTime);
            byte[] placementPolicyCheckFormatByteArray = fmt.toByteArray();
            if (zkc.exists(placementPolicyCheckCtimeZnode, false) != null) {
                zkc.setData(placementPolicyCheckCtimeZnode, placementPolicyCheckFormatByteArray, -1);
            } else {
                zkc.create(placementPolicyCheckCtimeZnode, placementPolicyCheckFormatByteArray, zkAcls,
                        CreateMode.PERSISTENT);
            }
        } catch (KeeperException ke) {
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public long getPlacementPolicyCheckCTime() throws UnavailableException {
        log.debug("getPlacementPolicyCheckCTime");
        try {
            byte[] data = zkc.getData(placementPolicyCheckCtimeZnode, false, null);
            PlacementPolicyCheckFormat placementPolicyCheckFormat = new PlacementPolicyCheckFormat();
            placementPolicyCheckFormat.parseFrom(data);
            return placementPolicyCheckFormat.hasPlacementPolicyCheckCTime()
                    ? placementPolicyCheckFormat.getPlacementPolicyCheckCTime() : -1;
        } catch (KeeperException.NoNodeException ne) {
            log.warn("placementPolicyCheckCtimeZnode is not yet available");
            return -1;
        } catch (KeeperException ke) {
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        } catch (RuntimeException ipbe) {
            throw new ReplicationException.UnavailableException("Error while parsing ZK protobuf binary data", ipbe);
        }
    }

    @Override
    public void setReplicasCheckCTime(long replicasCheckCTime) throws UnavailableException {
        try {
            List<ACL> zkAcls = ZkUtils.getACLs(conf);
            ReplicasCheckFormat fmt = new ReplicasCheckFormat();
            fmt.setReplicasCheckCTime(replicasCheckCTime);
            byte[] replicasCheckFormatByteArray = fmt.toByteArray();
            if (zkc.exists(replicasCheckCtimeZnode, false) != null) {
                zkc.setData(replicasCheckCtimeZnode, replicasCheckFormatByteArray, -1);
            } else {
                zkc.create(replicasCheckCtimeZnode, replicasCheckFormatByteArray, zkAcls, CreateMode.PERSISTENT);
            }
            log.debug("setReplicasCheckCTime completed successfully");
        } catch (KeeperException ke) {
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public long getReplicasCheckCTime() throws UnavailableException {
        try {
            byte[] data = zkc.getData(replicasCheckCtimeZnode, false, null);
            ReplicasCheckFormat replicasCheckFormat = new ReplicasCheckFormat();
            replicasCheckFormat.parseFrom(data);
            log.debug("getReplicasCheckCTime completed successfully");
            return replicasCheckFormat.hasReplicasCheckCTime() ? replicasCheckFormat.getReplicasCheckCTime() : -1;
        } catch (KeeperException.NoNodeException ne) {
            log.warn("replicasCheckCtimeZnode is not yet available");
            return -1;
        } catch (KeeperException ke) {
            throw ReplicationException.fromKeeperException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        } catch (RuntimeException ipbe) {
            throw new ReplicationException.UnavailableException("Error while parsing ZK protobuf binary data", ipbe);
        }
    }
}

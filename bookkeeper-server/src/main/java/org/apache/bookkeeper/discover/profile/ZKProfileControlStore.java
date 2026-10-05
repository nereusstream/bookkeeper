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
import java.util.Base64;
import java.util.HexFormat;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.common.profile.control.CommittedProfileAuthority;
import org.apache.bookkeeper.common.profile.control.ProfileAuthorityKey;
import org.apache.bookkeeper.common.profile.control.ProfileControlStore;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.Stat;

/**
 * Experimental cold reader of a provisioned committed-authority view namespace.
 * The caller owns its ZooKeeper client, publication protocol and namespace/ACLs.
 * No authority write/CAS, membership, fanout/page, backlink, retry or production endpoint is supplied.
 */
public final class ZKProfileControlStore implements ProfileControlStore {
    private final ProfileZKReadClient reads;
    private final String root;
    private final long timeoutNanos;

    public ZKProfileControlStore(ZooKeeper zk, String root, Duration timeout) {
        reads = new ProfileZKReadClient(zk);
        PathUtils.validatePath(root);
        if (root.equals("/") || timeout.isNegative() || timeout.isZero()
                || timeout.compareTo(Duration.ofMinutes(1)) > 0) {
            throw new IllegalArgumentException("Invalid authority namespace or timeout");
        }
        this.root = root;
        timeoutNanos = timeout.toNanos();
    }

    String path(ProfileAuthorityKey key) {
        Objects.requireNonNull(key, "key");
        byte[] domain = ProfileAuthorityCodec.text(key.authorityDomain(), 128);
        return root + "/" + Long.toHexString(key.ledgerId()) + "-"
                + HexFormat.of().formatHex(key.ledgerInstanceId()) + "-"
                + ProfileAuthorityCodec.purposeId(key.purpose()) + "-"
                + Base64.getUrlEncoder().withoutPadding().encodeToString(domain);
    }

    @Override
    public ReadResult read(ProfileAuthorityKey key) {
        Objects.requireNonNull(key, "key");
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            String path = path(key);
            if (reads.sync(root, deadline) != Code.OK.intValue()) {
                return ReadResult.unavailable();
            }
            ProfileZKReadClient.Node node = reads.get(path, deadline);
            if (node.code() == Code.NONODE.intValue()) {
                return ReadResult.notFound();
            }
            if (node.code() != Code.OK.intValue()) {
                return ReadResult.unavailable();
            }
            Stat stat = node.stat();
            if (stat == null || stat.getEphemeralOwner() != 0 || stat.getVersion() < 0
                    || stat.getCzxid() < 0 || stat.getMzxid() < stat.getCzxid()) {
                return ReadResult.quarantined();
            }
            CommittedProfileAuthority authority = ProfileAuthorityCodec.decode(node.bytes(), stat.getMzxid());
            if (!key.equals(ProfileAuthorityKey.fromScope(authority.scope()))
                    || !authority.permits(authority.scope().operation())) {
                return ReadResult.quarantined();
            }
            return ReadResult.found(authority);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return ReadResult.unavailable();
        } catch (TimeoutException timeout) {
            return ReadResult.unavailable();
        } catch (IllegalArgumentException invalid) {
            return ReadResult.quarantined();
        } catch (RuntimeException unavailable) {
            return ReadResult.unavailable();
        }
    }
}

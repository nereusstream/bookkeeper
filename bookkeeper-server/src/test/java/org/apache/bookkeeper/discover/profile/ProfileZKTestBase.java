/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.discover.profile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;

/** Shared real-server fixture; faults affect delivery, never replace the actual ZooKeeper transaction. */
abstract class ProfileZKTestBase {
    static final String ROOT = "/profile-readiness-test";
    static final String BOOKIE = "bookie:[::1]:3181";
    final TestCluster cluster = new TestCluster();
    TestClient first;
    TestClient second;

    @Before
    public void setup() throws Exception {
        cluster.startCluster();
        first = connect();
        second = connect();
        first.create(ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @After
    public void cleanup() throws Exception {
        try {
            if (first != null) {
                first.close();
            }
        } finally {
            try {
                if (second != null) {
                    second.close();
                }
            } finally {
                cluster.stopCluster();
            }
        }
    }

    private TestClient connect() throws Exception {
        CountDownLatch connected = new CountDownLatch(1);
        CountDownLatch expired = new CountDownLatch(1);
        TestClient client = new TestClient(cluster.getZooKeeperConnectString(), event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            } else if (event.getState() == Watcher.Event.KeeperState.Expired) {
                expired.countDown();
            }
        }, expired);
        if (!connected.await(10, TimeUnit.SECONDS)) {
            client.close();
            throw new IOException("ZooKeeper test client did not connect");
        }
        return client;
    }

    static final class TestCluster extends ZooKeeperUtil {
        @Override
        public String getZooKeeperConnectString() {
            String address = super.getZooKeeperConnectString();
            return "127.0.0.1" + address.substring(address.lastIndexOf(':'));
        }

        void expire(long sessionId) {
            zks.closeSession(sessionId);
        }
    }

    static final class TestClient extends ZooKeeper {
        final AtomicBoolean loseReply = new AtomicBoolean();
        final AtomicBoolean holdReply = new AtomicBoolean();
        final AtomicReference<Runnable> beforeMulti = new AtomicReference<>();
        final AtomicReference<Runnable> lateReply = new AtomicReference<>();
        final AtomicReference<List<Op>> lastOperations = new AtomicReference<>();
        final CountDownLatch replyHeld = new CountDownLatch(1);
        final CountDownLatch expired;
        final AtomicInteger reads = new AtomicInteger();
        final AtomicBoolean holdReadReply = new AtomicBoolean();
        final AtomicReference<Runnable> lateReadReply = new AtomicReference<>();
        final CountDownLatch readReplyHeld = new CountDownLatch(1);

        private TestClient(String connect, Watcher watcher, CountDownLatch expired) throws IOException {
            super(connect, 10000, watcher);
            this.expired = expired;
        }

        @Override
        public void getData(String path, boolean watch, DataCallback callback, Object context) {
            reads.incrementAndGet();
            boolean hold = holdReadReply.getAndSet(false);
            super.getData(path, watch, (code, returnedPath, ignored, bytes, stat) -> {
                Runnable reply = () -> callback.processResult(code, returnedPath, context, bytes, stat);
                if (hold) {
                    lateReadReply.set(reply);
                    readReplyHeld.countDown();
                } else {
                    reply.run();
                }
            }, context);
        }

        @Override
        public void multi(Iterable<Op> operations, MultiCallback callback, Object context) {
            List<Op> captured = new ArrayList<>();
            operations.forEach(captured::add);
            lastOperations.set(List.copyOf(captured));
            Runnable before = beforeMulti.getAndSet(null);
            if (before != null) {
                before.run();
            }
            boolean lose = loseReply.getAndSet(false);
            boolean hold = holdReply.getAndSet(false);
            super.multi(captured, (code, path, ignored, results) -> {
                Runnable reply = () -> callback.processResult(lose && code == Code.OK.intValue()
                        ? Code.CONNECTIONLOSS.intValue() : code, path, context, results);
                if (hold) {
                    lateReply.set(reply);
                    replyHeld.countDown();
                } else {
                    reply.run();
                }
            }, context);
        }
    }
}

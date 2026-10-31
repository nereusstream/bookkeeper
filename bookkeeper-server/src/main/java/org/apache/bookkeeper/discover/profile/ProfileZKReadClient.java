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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/** Shared bounded cold reads of a borrowed ZooKeeper client; no retry, cache, watcher or client ownership. */
final class ProfileZKReadClient {
    record Node(int code, byte[] bytes, Stat stat) { }
    record Existence(int code, Stat stat) { }

    /** An explicit cold owner retains even an abandoned or ambiguously submitted callback. */
    static final class Pending {
        private int callbacks;
        private boolean stopped;

        synchronized void submitted() {
            if (stopped) {
                throw new IllegalStateException("Cold metadata owner is closing");
            }
            callbacks++;
        }

        synchronized void returned() {
            callbacks--;
        }

        synchronized int stop() {
            stopped = true;
            return callbacks;
        }
    }

    private final ZooKeeper zk;
    private final Pending pending;

    ProfileZKReadClient(ZooKeeper zk) {
        this(zk, null);
    }

    ProfileZKReadClient(ZooKeeper zk, Pending pending) {
        this.zk = Objects.requireNonNull(zk, "zk");
        this.pending = pending;
    }

    private void submitted() {
        if (pending != null) {
            pending.submitted();
        }
    }

    private void returned() {
        if (pending != null) {
            pending.returned();
        }
    }

    Node get(String path, long deadline) throws InterruptedException, TimeoutException {
        requireTime(deadline);
        CompletableFuture<Node> reply = new CompletableFuture<>();
        submitted();
        zk.getData(path, false, (code, ignored, context, bytes, stat) -> {
            try {
                reply.complete(new Node(code, bytes, stat));
            } finally {
                returned();
            }
        }, null);
        return await(reply, deadline);
    }

    Existence exists(String path, long deadline) throws InterruptedException, TimeoutException {
        requireTime(deadline);
        CompletableFuture<Existence> reply = new CompletableFuture<>();
        submitted();
        zk.exists(path, false, (code, ignored, context, stat) -> {
            try {
                reply.complete(new Existence(code, stat));
            } finally {
                returned();
            }
        }, null);
        return await(reply, deadline);
    }

    int sync(String root, long deadline) throws InterruptedException, TimeoutException {
        requireTime(deadline);
        CompletableFuture<Integer> reply = new CompletableFuture<>();
        submitted();
        zk.sync(root, (code, ignored, context) -> {
            try {
                reply.complete(code);
            } finally {
                returned();
            }
        }, null);
        return await(reply, deadline);
    }

    static void requireTime(long deadline) throws TimeoutException {
        if (deadline - System.nanoTime() <= 0) {
            throw new TimeoutException("Profile metadata operation deadline elapsed");
        }
    }

    static <T> T await(CompletableFuture<T> reply, long deadline) throws InterruptedException, TimeoutException {
        try {
            return reply.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (ExecutionException impossible) {
            throw new IllegalStateException("Profile metadata callback completed exceptionally", impossible);
        }
    }
}

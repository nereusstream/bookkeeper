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
package org.apache.bookkeeper.bookie.storage.segment;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.proto.profile.ProfileReceiveBudget;
import org.apache.bookkeeper.tls.TLSContextFactory;

/** Explicit isolated Profile listener owner. Caller retains storage, TLS, cold pool and event-loop ownership. */
final class SegmentProfileListener {
    record Settings(int connectionBytes, int connectionBaseBytes, String trustDomain, long readinessGeneration) {
        Settings {
            if (connectionBytes < 1 || connectionBaseBytes < 1 || connectionBaseBytes > connectionBytes
                    || trustDomain == null || trustDomain.isEmpty() || readinessGeneration < 0) {
                throw new IllegalArgumentException("Invalid Profile listener settings");
            }
        }
    }

    private final EventLoopGroup worker;
    private final ProfileReceiveBudget budget;
    private final DefaultChannelGroup children;
    private final Channel listener;
    private final AtomicBoolean closing = new AtomicBoolean();
    private final CompletableFuture<InetSocketAddress> bound = new CompletableFuture<>();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();

    SegmentProfileListener(IsolatedSegmentStorage storage, TLSContextFactory tls, ProfileReceiveBudget budget,
            Settings settings, InetSocketAddress address, EventLoopGroup acceptor, EventLoopGroup worker,
            SegmentProfileControlConfig control, SegmentProfileReadConfig read) {
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(tls, "tls");
        Objects.requireNonNull(settings, "settings");
        Objects.requireNonNull(address, "address");
        Objects.requireNonNull(acceptor, "acceptor");
        this.worker = Objects.requireNonNull(worker, "worker");
        this.budget = Objects.requireNonNull(budget, "budget");
        children = new DefaultChannelGroup(worker.next(), true);
        ChannelFuture binding = new ServerBootstrap().group(acceptor, worker).channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        children.add(channel);
                        storage.installProfileConnection(channel.pipeline(), tls, budget, settings.connectionBytes(),
                                settings.connectionBaseBytes(), settings.trustDomain(), settings.readinessGeneration(),
                                control, read);
                    }
                }).bind(address);
        listener = binding.channel();
        binding.addListener(result -> {
            if (result.isSuccess() && !closing.get()) {
                bound.complete((InetSocketAddress) listener.localAddress());
            } else {
                Throwable failure = result.cause() == null
                        ? new IllegalStateException("Profile listener retired during bind") : result.cause();
                closeAsync().whenComplete((ignored, cleanup) -> {
                    if (cleanup != null) {
                        failure.addSuppressed(cleanup);
                    }
                    bound.completeExceptionally(failure);
                });
            }
        });
    }

    CompletableFuture<InetSocketAddress> bound() {
        return bound.copy();
    }

    /** Network/cold/response credit drain. Original storage I/O/owner drain remains the caller's next step. */
    CompletableFuture<Void> closeAsync() {
        if (closing.compareAndSet(false, true)) {
            var credits = budget.closeConnectionsAndDrain();
            // Stay-closed membership rejects a child even if registration was already queued on a worker.
            children.close();
            listener.close().addListener(parent -> {
                if (!parent.isSuccess()) {
                    drained.completeExceptionally(parent.cause());
                    return;
                }
                var barriers = new ArrayList<CompletableFuture<Void>>();
                for (var executor : worker) {
                    var barrier = new CompletableFuture<Void>();
                    barriers.add(barrier);
                    try {
                        executor.execute(() -> barrier.complete(null));
                    } catch (RuntimeException rejected) {
                        barrier.completeExceptionally(rejected);
                    }
                }
                CompletableFuture.allOf(barriers.toArray(CompletableFuture[]::new)).whenComplete((ignored, failed) -> {
                    if (failed != null) {
                        drained.completeExceptionally(failed);
                        return;
                    }
                    children.close().addListener(closed -> {
                        if (!closed.isSuccess()) {
                            drained.completeExceptionally(closed.cause());
                        } else {
                            credits.whenComplete((done, failure) -> {
                                if (failure == null) {
                                    drained.complete(null);
                                } else {
                                    drained.completeExceptionally(failure);
                                }
                            });
                        }
                    });
                });
            });
        }
        return drained.copy();
    }
}

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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.meta;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

/** Actual scheduler/thread ownership only; no mock I/O result or distributed storage proof. */
public class HierarchicalLedgerManagerDrainTest {
    private static HierarchicalLedgerManager manager() {
        return new HierarchicalLedgerManager(new ServerConfiguration()
                .setMetadataServiceUri("zk://127.0.0.1:1/manager-drain"), mock(ZooKeeper.class));
    }

    @Test(timeout = 10000)
    public void heldChildSchedulerPreventsDrainAndAllOriginalThreadsExitBeforeSuccess() throws Exception {
        var manager = manager();
        var release = new CountDownLatch(1);
        var entered = new CountDownLatch(1);
        var childThread = new java.util.concurrent.CompletableFuture<Thread>();
        Thread own = manager.scheduler.submit(Thread::currentThread).get(3, TimeUnit.SECONDS);
        Thread legacy = manager.legacyLM.scheduler.submit(Thread::currentThread).get(3, TimeUnit.SECONDS);
        manager.longLM.scheduler.execute(() -> {
            childThread.complete(Thread.currentThread());
            entered.countDown();
            try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException problem) {
                throw new AssertionError(problem);
            }
        });
        var closingWorker = Executors.newSingleThreadExecutor();
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            var closing = closingWorker.submit(() -> manager.closeAndAwaitTermination(Duration.ofSeconds(5)));
            assertThrows(TimeoutException.class, () -> closing.get(100, TimeUnit.MILLISECONDS));
            assertTrue(childThread.get().isAlive());
            release.countDown();
            assertTrue(closing.get(5, TimeUnit.SECONDS));
            assertFalse(own.isAlive());
            assertFalse(legacy.isAlive());
            assertFalse(childThread.get().isAlive());
            assertTrue(manager.closeAndAwaitTermination(Duration.ofSeconds(1)));
        } finally {
            release.countDown();
            manager.closeAndAwaitTermination(Duration.ofSeconds(5));
            closingWorker.shutdown();
            assertTrue(closingWorker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test(timeout = 10000)
    public void anyOfTheThreeActualSchedulersRejectsSelfAwaitBeforeAnyShutdown() throws Exception {
        var manager = manager();
        try {
            for (var source : List.of(manager, manager.legacyLM, manager.longLM)) {
                source.scheduler.submit(() -> assertThrows(IllegalStateException.class,
                        () -> manager.closeAndAwaitTermination(Duration.ofSeconds(1)))).get(3, TimeUnit.SECONDS);
                assertFalse(manager.scheduler.isShutdown());
                assertFalse(manager.legacyLM.scheduler.isShutdown());
                assertFalse(manager.longLM.scheduler.isShutdown());
            }
        } finally {
            assertTrue(manager.closeAndAwaitTermination(Duration.ofSeconds(5)));
        }
    }
}

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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/** Admission and actual local RPC custody for one view of a shared connection. */
final class ProfileRpcScope {
    private final Set<CompletableFuture<Void>> pending =
            Collections.newSetFromMap(new IdentityHashMap<>());
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private boolean closed;
    private Throwable failure;

    synchronized <T> T admit(Supplier<T> work, Function<T, CompletableFuture<Void>> terminal) {
        if (closed) {
            throw new IllegalStateException("Profile RPC scope is closed");
        }
        T admitted = work.get();
        CompletableFuture<Void> completion = Objects.requireNonNull(terminal.apply(admitted), "completion");
        pending.add(completion);
        completion.whenComplete((ignored, failed) -> completed(completion, failed));
        return admitted;
    }

    CompletableFuture<Void> closeAsync() {
        boolean complete;
        Throwable failed;
        synchronized (this) {
            closed = true;
            complete = pending.isEmpty();
            failed = failure;
        }
        if (complete) {
            finish(failed);
        }
        return drained.copy();
    }

    private void completed(CompletableFuture<Void> completion, Throwable failed) {
        boolean complete;
        Throwable terminalFailure;
        synchronized (this) {
            pending.remove(completion);
            if (failed != null && failure == null) {
                failure = failed;
            }
            complete = closed && pending.isEmpty();
            terminalFailure = failure;
        }
        if (complete) {
            finish(terminalFailure);
        }
    }

    private void finish(Throwable failed) {
        if (failed == null) {
            drained.complete(null);
        } else {
            drained.completeExceptionally(failed);
        }
    }
}

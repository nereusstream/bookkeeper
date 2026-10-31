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
package org.apache.bookkeeper.server;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.bookie.storage.segment.SegmentClassicLauncher;

/** Fixed, separately launched UID-zero Classic launcher; never the default Bookie entry. */
public final class ControlledClassicLauncherMain {
    private ControlledClassicLauncherMain() { }

    public static void main(String[] arguments) throws Exception {
        // Protocol output uses FileDescriptor.out directly. Redirect before any business/logging initialization.
        System.setOut(System.err);
        if (arguments.length != 1) {
            throw new IllegalArgumentException("One installed Classic recipe ID required");
        }
        var owner = SegmentClassicLauncher.create(arguments[0]);
        boolean interrupted = false;
        try {
            owner.start();
        } catch (InterruptedException problem) {
            interrupted = true;
            owner.requestClose();
        } catch (Exception | Error problem) {
            problem.printStackTrace(System.err);
            owner.requestClose();
        }
        SegmentClassicLauncher.Status result;
        for (;;) {
            try {
                result = owner.awaitTermination(Duration.ofSeconds(30));
                break;
            } catch (TimeoutException pending) {
                // Observe the same episode again. A wait deadline is not a service lifetime or I/O cancellation.
            } catch (InterruptedException problem) {
                interrupted = true;
                owner.requestClose();
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        if (interrupted || result.failure() != null || result.state() != SegmentClassicLauncher.State.DRAINED) {
            throw new IllegalStateException("Controlled Classic launcher episode failed", result.failure());
        }
    }
}

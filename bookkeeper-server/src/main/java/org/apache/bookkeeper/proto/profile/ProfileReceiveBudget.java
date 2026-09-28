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
package org.apache.bookkeeper.proto.profile;

/** One bounded receive ledger shared by DATA and reserved CONTROL connections. */
final class ProfileReceiveBudget {
    enum Lane { DATA, CONTROL }

    final class Credit implements AutoCloseable {
        private final Lane lane;
        private final int bytes;
        private boolean closed;

        private Credit(Lane lane, int bytes) {
            this.lane = lane;
            this.bytes = bytes;
        }

        @Override
        public void close() {
            synchronized (ProfileReceiveBudget.this) {
                if (closed) {
                    throw new IllegalStateException("Receive credit returned twice");
                }
                closed = true;
                if (lane == Lane.DATA) {
                    dataBytes -= bytes;
                } else {
                    controlBytes -= bytes;
                }
            }
        }
    }

    private final long dataLimit;
    private final long controlLimit;
    private long dataBytes;
    private long controlBytes;

    ProfileReceiveBudget(long totalBytes, long reservedControlBytes) {
        if (totalBytes < 1 || reservedControlBytes < 1 || reservedControlBytes >= totalBytes) {
            throw new IllegalArgumentException("Positive DATA and reserved CONTROL receive budgets required");
        }
        dataLimit = totalBytes - reservedControlBytes;
        controlLimit = reservedControlBytes;
    }

    synchronized Credit reserve(Lane lane, int bytes) {
        if (lane == null || bytes < 0) {
            throw new IllegalArgumentException("Invalid receive credit request");
        }
        if (lane == Lane.DATA) {
            if (bytes > dataLimit - dataBytes) {
                return null;
            }
            dataBytes += bytes;
        } else {
            if (bytes > controlLimit - controlBytes) {
                return null;
            }
            controlBytes += bytes;
        }
        return new Credit(lane, bytes);
    }

    synchronized long used(Lane lane) {
        if (lane == null) {
            throw new IllegalArgumentException("Receive lane required");
        }
        return lane == Lane.DATA ? dataBytes : controlBytes;
    }
}

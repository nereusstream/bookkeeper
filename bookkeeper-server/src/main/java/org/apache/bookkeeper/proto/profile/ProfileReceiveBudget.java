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

/** One bounded receive ledger shared by unclassified, DATA and reserved CONTROL connections. */
final class ProfileReceiveBudget {
    enum Lane { UNCLASSIFIED, DATA, CONTROL }

    final class Credit implements AutoCloseable {
        private final ProfileReceiveBudget owner = ProfileReceiveBudget.this;
        private Lane lane;
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
                usedBytes[lane.ordinal()] -= bytes;
            }
        }
    }

    private final long[] limits = new long[Lane.values().length];
    private final long[] usedBytes = new long[Lane.values().length];

    ProfileReceiveBudget(long totalBytes, long reservedControlBytes, long unclassifiedBytes) {
        if (totalBytes < 3 || reservedControlBytes < 1 || reservedControlBytes >= totalBytes
                || unclassifiedBytes < 1 || unclassifiedBytes >= totalBytes - reservedControlBytes) {
            throw new IllegalArgumentException("Positive unclassified, DATA and CONTROL budgets required");
        }
        limits[Lane.UNCLASSIFIED.ordinal()] = unclassifiedBytes;
        limits[Lane.DATA.ordinal()] = totalBytes - reservedControlBytes - unclassifiedBytes;
        limits[Lane.CONTROL.ordinal()] = reservedControlBytes;
    }

    synchronized Credit reserve(Lane lane, int bytes) {
        if (lane == null || bytes < 0) {
            throw new IllegalArgumentException("Invalid receive credit request");
        }
        int index = lane.ordinal();
        if (bytes > limits[index] - usedBytes[index]) {
            return null;
        }
        usedBytes[index] += bytes;
        return new Credit(lane, bytes);
    }

    /** Move existing ownership without a release-and-reserve gap. A failed move leaves every credit unchanged. */
    synchronized boolean transfer(Lane target, Credit... credits) {
        if (target == null || credits.length == 0 || credits[0] == null) {
            throw new IllegalArgumentException("Receive target and credits required");
        }
        Lane source = credits[0].lane;
        long bytes = 0;
        for (int i = 0; i < credits.length; i++) {
            Credit credit = credits[i];
            if (credit == null || credit.owner != this || credit.closed || credit.lane != source) {
                throw new IllegalStateException("Invalid receive credit transfer");
            }
            for (int previous = 0; previous < i; previous++) {
                if (credit == credits[previous]) {
                    throw new IllegalStateException("Receive credit transferred twice");
                }
            }
            bytes += credit.bytes;
        }
        if (source == target || bytes > limits[target.ordinal()] - usedBytes[target.ordinal()]) {
            return false;
        }
        usedBytes[source.ordinal()] -= bytes;
        usedBytes[target.ordinal()] += bytes;
        for (Credit credit : credits) {
            credit.lane = target;
        }
        return true;
    }

    synchronized long used(Lane lane) {
        if (lane == null) {
            throw new IllegalArgumentException("Receive lane required");
        }
        return usedBytes[lane.ordinal()];
    }
}

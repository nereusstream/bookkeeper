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
package org.apache.bookkeeper.bookie.storage.segment;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** One blocking FileChannel backend, with finite deterministic fault points for isolated tests. */
final class SegmentFileIo {
    private final int maxWrite;
    private final int failWrite;
    private final int failForce;
    private final boolean zeroWrites;
    int writes;
    int forces;

    SegmentFileIo() {
        this(Integer.MAX_VALUE, -1, -1, false);
    }

    SegmentFileIo(int maxWrite, int failWrite, int failForce, boolean zeroWrites) {
        if (maxWrite <= 0) {
            throw new IllegalArgumentException("Positive transfer bound required");
        }
        this.maxWrite = maxWrite;
        this.failWrite = failWrite;
        this.failForce = failForce;
        this.zeroWrites = zeroWrites;
    }

    void writeFully(FileChannel channel, ByteBuffer input, long offset) throws IOException {
        ByteBuffer bytes = input.duplicate();
        if (offset < 0 || offset > Long.MAX_VALUE - bytes.remaining()) {
            throw new IOException("Invalid write range");
        }
        int zeroProgress = 0;
        while (bytes.hasRemaining()) {
            if (++writes == failWrite) {
                throw new IOException("Injected write failure");
            }
            int limit = bytes.limit();
            bytes.limit(bytes.position() + Math.min(bytes.remaining(), maxWrite));
            int n;
            try {
                n = zeroWrites ? 0 : channel.write(bytes, offset);
            } finally {
                bytes.limit(limit);
            }
            if (n == 0) {
                if (++zeroProgress >= 16) {
                    throw new IOException("File write made no progress");
                }
            } else {
                zeroProgress = 0;
                offset += n;
            }
        }
    }

    void force(FileChannel channel, boolean metadata) throws IOException {
        if (++forces == failForce) {
            throw new IOException("Injected force failure");
        }
        channel.force(metadata);
    }

    static void readFully(FileChannel channel, ByteBuffer target, long offset) throws IOException {
        if (offset < 0 || offset > Long.MAX_VALUE - target.remaining()) {
            throw new IOException("Invalid read range");
        }
        int zeroProgress = 0;
        while (target.hasRemaining()) {
            int n = channel.read(target, offset);
            if (n < 0) {
                throw new EOFException("Truncated segment file");
            }
            if (n == 0) {
                if (++zeroProgress >= 16) {
                    throw new IOException("File read made no progress");
                }
            } else {
                zeroProgress = 0;
                offset += n;
            }
        }
        target.flip();
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

/** Bounded bytes on an original Unix channel and dedicated selector; never authenticates or accepts an operation. */
final class SegmentClassicLauncherOperationIo {
    private SegmentClassicLauncherOperationIo() { }

    /** One connection attempt only; timeout leaves the original resources with their owner. */
    static void connect(SocketChannel channel, Selector selector, UnixDomainSocketAddress endpoint, long deadline)
            throws IOException {
        SelectionKey key = originalKey(channel, selector);
        if (channel.isConnected() || channel.isConnectionPending()) {
            throw new IOException("Operation channel connection attempt has already started");
        }
        remaining(deadline);
        if (!channel.connect(endpoint)) {
            while (!channel.finishConnect()) {
                await(key, selector, SelectionKey.OP_CONNECT, deadline);
                remaining(deadline);
            }
        }
        remaining(deadline);
    }

    static SegmentClassicLauncherOperationCodec.Frame read(SocketChannel channel, Selector selector, long deadline)
            throws IOException {
        SelectionKey key = connectedKey(channel, selector);
        ByteBuffer header = ByteBuffer.allocate(SegmentClassicLauncherOperationCodec.HEADER_BYTES);
        readFully(channel, key, selector, header, deadline);
        int body = SegmentClassicLauncherOperationCodec.bodyBytes(header.array());
        // A header never controls an unchecked allocation or extends the original operation deadline.
        ByteBuffer frame = ByteBuffer.allocate(SegmentClassicLauncherOperationCodec.HEADER_BYTES + body);
        frame.put(header.array());
        readFully(channel, key, selector, frame, deadline);
        return SegmentClassicLauncherOperationCodec.decode(frame.array());
    }

    static void write(SocketChannel channel, Selector selector, SegmentClassicLauncherOperationCodec.Frame frame,
                      long deadline) throws IOException {
        SelectionKey key = connectedKey(channel, selector);
        ByteBuffer bytes = ByteBuffer.wrap(SegmentClassicLauncherOperationCodec.encode(frame));
        while (bytes.hasRemaining()) {
            remaining(deadline);
            if (channel.write(bytes) == 0) {
                await(key, selector, SelectionKey.OP_WRITE, deadline);
            }
        }
    }

    private static void readFully(SocketChannel channel, SelectionKey key, Selector selector, ByteBuffer bytes,
                                  long deadline) throws IOException {
        while (bytes.hasRemaining()) {
            remaining(deadline);
            int read = channel.read(bytes);
            if (read < 0) {
                throw new EOFException("Original operation channel ended before a complete frame");
            }
            if (read == 0) {
                await(key, selector, SelectionKey.OP_READ, deadline);
            }
        }
    }

    private static SelectionKey connectedKey(SocketChannel channel, Selector selector) throws IOException {
        SelectionKey key = originalKey(channel, selector);
        if (!channel.isConnected()) {
            throw new IOException("Original operation channel is not connected");
        }
        if (!(channel.getRemoteAddress() instanceof UnixDomainSocketAddress)) {
            throw new IOException("Only a Unix domain operation channel is supported");
        }
        return key;
    }

    private static SelectionKey originalKey(SocketChannel channel, Selector selector) throws IOException {
        if (!channel.isOpen() || channel.isBlocking() || !selector.isOpen()) {
            throw new IOException("Original operation channel and selector must remain open and nonblocking");
        }
        SelectionKey key = channel.keyFor(selector);
        if (key == null || !key.isValid() || selector.keys().size() != 1) {
            throw new IOException("Original operation channel registration is unavailable");
        }
        return key;
    }

    private static void await(SelectionKey key, Selector selector, int interest, long deadline) throws IOException {
        long wait = remaining(deadline);
        try {
            key.interestOps(interest);
            try {
                selector.select(Math.max(1, Math.min(1000, TimeUnit.NANOSECONDS.toMillis(wait))));
                selector.selectedKeys().clear();
            } finally {
                if (key.isValid()) {
                    key.interestOps(0);
                }
            }
        } catch (CancelledKeyException cancelled) {
            throw new IOException("Original operation channel registration ended", cancelled);
        }
    }

    private static long remaining(long deadline) throws IOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException("Original operation byte exchange was interrupted");
        }
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new SocketTimeoutException("Original operation byte-exchange deadline elapsed");
        }
        return remaining;
    }
}

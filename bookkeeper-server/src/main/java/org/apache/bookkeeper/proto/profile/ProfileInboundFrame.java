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

import io.netty.buffer.ByteBuf;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameCodec;
import org.apache.bookkeeper.common.profile.wire.ProfileFrameHeader;

/** One charged frame. The receiving handler owns and closes it after its final use. */
public final class ProfileInboundFrame implements AutoCloseable {
    private final ProfileFrameHeader header;
    private final ByteBuf bytes;
    private final Runnable releaseCredit;
    private final AtomicBoolean closed = new AtomicBoolean();

    ProfileInboundFrame(ProfileFrameHeader header, ByteBuf bytes, Runnable releaseCredit) {
        this.header = Objects.requireNonNull(header, "header");
        this.bytes = Objects.requireNonNull(bytes, "bytes");
        this.releaseCredit = Objects.requireNonNull(releaseCredit, "releaseCredit");
    }

    public ProfileFrameHeader header() {
        return header;
    }

    /** View shares this frame's lifetime; consumers must keep the frame until work using the view finishes. */
    public ByteBuf body() {
        if (closed.get()) {
            throw new IllegalStateException("Profile frame already released");
        }
        return bytes.slice(ProfileFrameCodec.OUTER_LENGTH_PREFIX + ProfileFrameHeader.ENCODED_LENGTH,
                header.bodyLength()).asReadOnly();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            throw new IllegalStateException("Profile frame released twice");
        }
        try {
            bytes.release();
        } finally {
            releaseCredit.run();
        }
    }
}

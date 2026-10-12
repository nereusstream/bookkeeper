/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.discover.profile;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Bounded immutable complete delete prefix. It neither changes assignment nor grants journal GC. */
public final class ProfileDeleteSnapshot {
    public static final int EVENTS_PER_PAGE = 64;
    public static final int MAX_GENERATIONS = 64;
    public static final int MAX_PAGES = (ProfileDeleteDelivery.MAX_EVENTS + EVENTS_PER_PAGE - 1) / EVENTS_PER_PAGE;
    static final int MAX_ROOT_BYTES = 44 + ProfileDeleteDelivery.MAX_BYTES + 32 * MAX_PAGES;
    static final int MAX_PAGE_BYTES = 36 + EVENTS_PER_PAGE * (12 + ProfileDeleteDelivery.MAX_BYTES);

    private ProfileDeleteSnapshot() { }

    public record Effect(long originalBirth, ProfileDeleteDelivery.Event event) {
        public Effect {
            Objects.requireNonNull(event, "event");
            if (originalBirth <= 0) {
                throw invalid();
            }
        }
    }

    public record Root(byte[] birthId, long generation, ProfileDeleteDelivery.Head cut, List<byte[]> pageDigests) {
        public Root {
            birthId = ProfileLifecycleHead.fixed(birthId, 16, true);
            Objects.requireNonNull(cut, "cut");
            pageDigests = pageDigests.stream().map(value -> ProfileLifecycleHead.fixed(value, 32, false)).toList();
            if (generation < 1 || generation > MAX_GENERATIONS || cut.committedThrough() < 1
                    || pageDigests.size() != (cut.committedThrough() + EVENTS_PER_PAGE - 1) / EVENTS_PER_PAGE) {
                throw invalid();
            }
        }
        @Override
        public byte[] birthId() {
            return birthId.clone();
        }
        @Override
        public List<byte[]> pageDigests() {
            return pageDigests.stream().map(byte[]::clone).toList();
        }
        public byte[] encode() {
            byte[] head = cut.encode();
            var out = header(44 + head.length + 32 * pageDigests.size(), 0x55574458)
                    .put(birthId).putLong(generation).putInt(head.length).put(head).putInt(pageDigests.size());
            pageDigests.forEach(out::put);
            return finish(out);
        }
        public static Root decode(byte[] bytes) {
            var in = checked(bytes, 44 + 71 + 32, MAX_ROOT_BYTES, 0x55574458);
            byte[] birth = new byte[16];
            in.get(birth);
            long generation = in.getLong();
            int length = in.getInt();
            if (length < 71 || length > ProfileDeleteDelivery.MAX_BYTES || in.remaining() < length + 8) {
                throw invalid();
            }
            byte[] head = new byte[length];
            in.get(head);
            int count = in.getInt();
            if (count < 1 || count > MAX_PAGES || in.remaining() != 32 * count + 4) {
                throw invalid();
            }
            var digests = new ArrayList<byte[]>(count);
            for (int index = 0; index < count; index++) {
                byte[] digest = new byte[32];
                in.get(digest);
                digests.add(digest);
            }
            var result = new Root(birth, generation, ProfileDeleteDelivery.Head.decode(head), digests);
            if (!Arrays.equals(bytes, result.encode())) {
                throw invalid();
            }
            return result;
        }
    }

    public record Page(byte[] birthId, int index, List<Effect> effects) {
        public Page {
            birthId = ProfileLifecycleHead.fixed(birthId, 16, true);
            effects = List.copyOf(effects);
            if (index < 0 || index >= MAX_PAGES || effects.isEmpty() || effects.size() > EVENTS_PER_PAGE) {
                throw invalid();
            }
            for (int slot = 0; slot < effects.size(); slot++) {
                if (effects.get(slot).event().sequence() != (long) index * EVENTS_PER_PAGE + slot + 1) {
                    throw invalid();
                }
            }
        }
        @Override
        public byte[] birthId() {
            return birthId.clone();
        }
        public byte[] encode() {
            int size = 36;
            for (var effect : effects) {
                size += 12 + effect.event().encode().length;
            }
            var out = header(size, 0x55574459).put(birthId).putInt(index).putInt(effects.size());
            for (var effect : effects) {
                byte[] event = effect.event().encode();
                out.putLong(effect.originalBirth()).putInt(event.length).put(event);
            }
            return finish(out);
        }
        public static Page decode(byte[] bytes) {
            var in = checked(bytes, 36 + 12 + 1, MAX_PAGE_BYTES, 0x55574459);
            byte[] birth = new byte[16];
            in.get(birth);
            int index = in.getInt();
            int count = in.getInt();
            if (count < 1 || count > EVENTS_PER_PAGE || in.remaining() < count * 13 + 4) {
                throw invalid();
            }
            var effects = new ArrayList<Effect>(count);
            for (int slot = 0; slot < count; slot++) {
                long originalBirth = in.getLong();
                int length = in.getInt();
                if (length < 1 || length > ProfileDeleteDelivery.MAX_BYTES
                        || in.remaining() < length + (count - slot - 1) * 13 + 4) {
                    throw invalid();
                }
                byte[] event = new byte[length];
                in.get(event);
                effects.add(new Effect(originalBirth, ProfileDeleteDelivery.Event.decode(event)));
            }
            if (in.remaining() != 4) {
                throw invalid();
            }
            var result = new Page(birth, index, effects);
            if (!Arrays.equals(bytes, result.encode())) {
                throw invalid();
            }
            return result;
        }
        public boolean matches(Root root) {
            int expected = (int) Math.min(EVENTS_PER_PAGE,
                    root.cut().committedThrough() - (long) index * EVENTS_PER_PAGE);
            return index < root.pageDigests.size() && Arrays.equals(birthId, root.birthId)
                    && effects.size() == expected
                    && Arrays.equals(ProfileRepairIntent.digest(encode()), root.pageDigests.get(index))
                    && effects.stream().allMatch(effect ->
                        Arrays.equals(effect.event().streamId(), root.cut().streamId())
                        && effect.event().streamGeneration() == root.cut().streamGeneration()
                        && effect.event().target().equals(root.cut().target())
                        && effect.event().assignmentGeneration() <= root.cut().assignmentGeneration()
                        && effect.event().sequence() <= root.cut().committedThrough());
        }
    }

    private static ByteBuffer header(int size, int magic) {
        return ByteBuffer.allocate(size).putInt(magic).putShort((short) 1).putShort((short) 0);
    }

    private static byte[] finish(ByteBuffer out) {
        if (out.position() != out.capacity() - 4) {
            throw invalid();
        }
        CRC32C crc = new CRC32C();
        crc.update(out.array(), 0, out.capacity() - 4);
        return out.putInt((int) crc.getValue()).array();
    }

    private static ByteBuffer checked(byte[] bytes, int minimum, int maximum, int magic) {
        if (bytes == null || bytes.length < minimum || bytes.length > maximum) {
            throw invalid();
        }
        var in = ByteBuffer.wrap(bytes);
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length - 4);
        if (in.getInt(bytes.length - 4) != (int) crc.getValue() || in.getInt() != magic
                || in.getShort() != 1 || in.getShort() != 0) {
            throw invalid();
        }
        return in;
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("Invalid complete bounded Profile delete snapshot");
    }
}

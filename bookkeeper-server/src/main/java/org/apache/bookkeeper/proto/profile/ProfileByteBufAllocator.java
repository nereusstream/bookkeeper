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

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.netty.buffer.UnpooledHeapByteBuf;

/** Unpooled backing capacities charged before allocation, including old plus new storage during resize. */
final class ProfileByteBufAllocator extends AbstractByteBufAllocator {
    private final ProfileReceiveBudget.ConnectionPermit connection;

    ProfileByteBufAllocator(ProfileReceiveBudget.ConnectionPermit connection) {
        super(true);
        this.connection = connection;
    }

    private ProfileReceiveBudget.Credit reserve(int bytes) {
        ProfileReceiveBudget.Credit credit = connection.reserve(bytes);
        if (credit == null) {
            throw new IllegalStateException("Profile backing buffer capacity exhausted or connection closed");
        }
        return credit;
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
        ProfileReceiveBudget.Credit credit = reserve(initialCapacity);
        try {
            return new ChargedHeap(initialCapacity, maxCapacity, credit);
        } catch (RuntimeException | Error failure) {
            credit.close();
            throw failure;
        }
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
        ProfileReceiveBudget.Credit credit = reserve(initialCapacity);
        try {
            return new ChargedDirect(initialCapacity, maxCapacity, credit);
        } catch (RuntimeException | Error failure) {
            credit.close();
            throw failure;
        }
    }

    /** Caller retains its reservation if construction fails; the root owns it only on success. */
    ByteBuf adopt(ProfileReceiveBudget.Credit credit, int capacity) {
        credit.require(connection, capacity);
        return new ChargedDirect(capacity, capacity, credit);
    }

    boolean owns(ByteBuf buffer) {
        ByteBuf root = buffer;
        while (root.unwrap() != null) {
            root = root.unwrap();
        }
        return root.alloc() == this && (root instanceof ChargedHeap || root instanceof ChargedDirect);
    }

    @Override
    public boolean isDirectBufferPooled() {
        return false;
    }

    private final class ChargedHeap extends UnpooledHeapByteBuf {
        private ProfileReceiveBudget.Credit credit;

        private ChargedHeap(int initialCapacity, int maxCapacity, ProfileReceiveBudget.Credit credit) {
            super(ProfileByteBufAllocator.this, initialCapacity, maxCapacity);
            this.credit = credit;
        }

        @Override
        public ByteBuf capacity(int capacity) {
            checkNewCapacity(capacity);
            if (capacity == capacity()) {
                return this;
            }
            ProfileReceiveBudget.Credit replacement = reserve(capacity);
            int reader = readerIndex();
            int writer = writerIndex();
            try {
                super.capacity(capacity);
            } catch (RuntimeException | Error failure) {
                setIndex(reader, writer);
                replacement.close();
                throw failure;
            }
            credit.close();
            credit = replacement;
            return this;
        }

        @Override
        protected void deallocate() {
            super.deallocate();
            credit.close();
        }
    }

    private final class ChargedDirect extends UnpooledDirectByteBuf {
        private ProfileReceiveBudget.Credit credit;

        private ChargedDirect(int initialCapacity, int maxCapacity, ProfileReceiveBudget.Credit credit) {
            super(ProfileByteBufAllocator.this, initialCapacity, maxCapacity);
            this.credit = credit;
        }

        @Override
        public ByteBuf capacity(int capacity) {
            checkNewCapacity(capacity);
            if (capacity == capacity()) {
                return this;
            }
            ProfileReceiveBudget.Credit replacement = reserve(capacity);
            int reader = readerIndex();
            int writer = writerIndex();
            try {
                super.capacity(capacity);
            } catch (RuntimeException | Error failure) {
                setIndex(reader, writer);
                replacement.close();
                throw failure;
            }
            credit.close();
            credit = replacement;
            return this;
        }

        @Override
        protected void deallocate() {
            super.deallocate();
            credit.close();
        }
    }
}

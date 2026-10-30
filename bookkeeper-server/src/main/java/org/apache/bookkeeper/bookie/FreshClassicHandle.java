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
package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;

/** Local operations only. Each write consumes one supplied ByteBuf reference, including rejection. */
public final class FreshClassicHandle {
    private final FreshClassicOwner owner;

    FreshClassicHandle(FreshClassicOwner owner) {
        this.owner = owner;
    }

    private BookieImpl writer(ByteBuf entry) {
        try {
            return owner.implementation();
        } catch (RuntimeException | Error failure) {
            entry.release();
            throw failure;
        }
    }

    public void addEntry(ByteBuf entry, WriteCallback callback, Object context, byte[] masterKey)
            throws IOException, InterruptedException, BookieException {
        writer(entry).addEntry(entry, false, callback, context, masterKey);
    }

    public void recoveryAddEntry(ByteBuf entry, WriteCallback callback, Object context, byte[] masterKey)
            throws IOException, InterruptedException, BookieException {
        writer(entry).recoveryAddEntry(entry, callback, context, masterKey);
    }

    public void setExplicitLac(ByteBuf entry, WriteCallback callback, Object context, byte[] masterKey)
            throws IOException, InterruptedException, BookieException {
        BookieImpl bookie = writer(entry);
        ByteBuf readable;
        try {
            readable = entry.slice(entry.readerIndex(), entry.readableBytes());
        } catch (RuntimeException | Error failure) {
            entry.release();
            throw failure;
        }
        bookie.setExplicitLac(readable, callback, context, masterKey);
    }

    public void forceLedger(long ledgerId, WriteCallback callback, Object context) {
        owner.implementation().forceLedger(ledgerId, callback, context);
    }

    public CompletableFuture<Boolean> fenceLedger(long ledgerId, byte[] masterKey) throws IOException, BookieException {
        return owner.implementation().fenceLedger(ledgerId, masterKey);
    }

    public ByteBuf readEntry(long ledgerId, long entryId) throws IOException, BookieException {
        return owner.implementation().readEntry(ledgerId, entryId);
    }

    public long readLastAddConfirmed(long ledgerId) throws IOException, BookieException {
        return owner.implementation().readLastAddConfirmed(ledgerId);
    }

    public ByteBuf getExplicitLac(long ledgerId) throws IOException, BookieException {
        return owner.implementation().getExplicitLac(ledgerId);
    }

    public FreshClassicOwner.Status status() {
        return owner.status();
    }
}

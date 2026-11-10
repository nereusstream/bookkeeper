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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.bookkeeper.common.profile.startup.ManifestIdentity;
import org.apache.bookkeeper.common.profile.startup.StorageIncarnation;
import org.apache.bookkeeper.common.profile.wire.ProfileLedgerContext;
import org.apache.bookkeeper.common.util.nativeio.NativeIO.FileIdentity;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction.FileImage;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction.HistoryMode;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction.Image;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction.Kind;
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction.Member;
import org.apache.bookkeeper.discover.profile.ProfileManifestClosedAdmission.Arena;
import org.apache.bookkeeper.discover.profile.ProfileManifestClosedAdmission.ArenaEpoch;
import org.apache.bookkeeper.discover.profile.ProfileManifestClosedAdmission.ControlPoint;
import org.apache.bookkeeper.discover.profile.ProfileManifestClosedAdmission.Ledger;
import org.apache.bookkeeper.discover.profile.ProfileManifestClosedAdmission.Scope;
import org.junit.Test;

public class ProfileManifestClosedAdmissionTest {
    private static final String SHA = "17".repeat(32);

    @Test
    public void maximumCanonicalVectorFitsAndKeepsItsExactEncoding() throws Exception {
        var epochs = new ArrayList<ArenaEpoch>();
        var arenas = new ArrayList<Arena>();
        for (int arena = 0; arena < 64; arena++) {
            long device = arena + 1L;
            epochs.add(new ArenaEpoch(new UUID(0, device), 2));
            var files = new ArrayList<FileImage>();
            for (int file = 0; file < 64; file++) {
                files.add(new FileImage(file, new Image(new FileIdentity(device, file + 2), 4096, SHA)));
            }
            arenas.add(new Arena(point(device, 1), arena == 0, HistoryMode.ORIGINAL, files));
        }
        var ledgers = new ArrayList<Ledger>();
        for (int ledger = 0; ledger < 16; ledger++) {
            ledgers.add(ledger(ledger, 48 + ledger, 48 + ledger));
        }
        var admission = new ProfileManifestClosedAdmission(scope("x".repeat(255), epochs), new UUID(0, 33),
                11, 12, 13, point(1, 100), arenas, ledgers);
        byte[] bytes = admission.encode();
        assertEquals(242523, bytes.length);
        assertEquals(admission, ProfileManifestClosedAdmission.decode(bytes));
        assertArrayEquals(bytes, ProfileManifestClosedAdmission.decode(bytes).encode());
        assertThrows(IOException.class, () -> ProfileClosedReconstruction.Admission.decode(bytes));
    }

    @Test
    public void fixedSlotAndDecoderRejectNoncanonicalEvidence() throws Exception {
        var first = new ArenaEpoch(new UUID(0, 1), 2);
        var second = new ArenaEpoch(new UUID(Long.MIN_VALUE, 1), 2);
        var scope = scope("bookie:3181", List.of(first, second));
        assertNotEquals(scope.key(), scope("bookie:3181",
                List.of(first, new ArenaEpoch(second.id(), 3))).key());
        assertThrows(IllegalArgumentException.class, () -> scope("bookie:3181", List.of(second, first)));
        assertThrows(IllegalArgumentException.class, () -> scope("bookie:3181", List.of(first, first)));
        var file = new FileImage(0, new Image(new FileIdentity(1, 2), 4096, SHA));
        var firstArena = new Arena(point(1, 1), false, HistoryMode.ORIGINAL, List.of(file));
        var secondArena = new Arena(point(2, 1), false, HistoryMode.ORIGINAL,
                List.of(new FileImage(0, new Image(new FileIdentity(2, 2), 4096, SHA))));
        var arenas = List.of(firstArena, secondArena);
        var bookie = point(1, 10);
        var member = ledger(1, 0, 0);
        var admission = new ProfileManifestClosedAdmission(scope, new UUID(0, 4), 11, 12, 13, bookie,
                arenas, List.of(member));
        byte[] valid = admission.encode();
        byte[] futureVersion = valid.clone();
        ByteBuffer.wrap(futureVersion).putInt(Integer.BYTES, 2);
        assertThrows(IOException.class, () -> ProfileManifestClosedAdmission.decode(futureVersion));
        assertThrows(IOException.class, () -> ProfileManifestClosedAdmission.decode(
                Arrays.copyOf(valid, valid.length - 1)));
        assertThrows(IOException.class, () -> ProfileManifestClosedAdmission.decode(
                Arrays.copyOf(valid, valid.length + 1)));
        assertThrows(IOException.class, () -> ProfileManifestClosedAdmission.decode(
                new byte[ProfileManifestClosedAdmission.MAX_BYTES + 1]));
        assertThrows(IllegalArgumentException.class, () -> new ProfileManifestClosedAdmission(
                scope, new UUID(0, 4), 11, 12, 13, point(1, 2), arenas, List.of(member)));
        assertThrows(IllegalArgumentException.class, () -> new ProfileManifestClosedAdmission(
                scope, new UUID(0, 4), 11, 12, 13, bookie, arenas, List.of(member, member)));
        assertThrows(IllegalArgumentException.class, () -> new ProfileManifestClosedAdmission(
                scope, new UUID(0, 4), 11, 12, 13, bookie, arenas, List.of(member, ledger(2, 0, 1))));
    }

    private static ControlPoint point(long device, long inode) {
        return new ControlPoint(new FileIdentity(device, inode), 2, 3, 1, SHA);
    }

    private static Ledger ledger(long id, int shard, int arena) {
        var context = new ProfileLedgerContext(id, bytes(16), bytes(36));
        return new Ledger(new Member(id, context, SHA, SHA, Kind.NONEMPTY_SELF), shard, arena, 0, 10);
    }

    private static Scope scope(String bookie, List<ArenaEpoch> epochs) {
        return new Scope(bookie, new StorageIncarnation(bytes(16)), new ManifestIdentity(bytes(32)), epochs);
    }

    private static byte[] bytes(int length) {
        byte[] value = new byte[length];
        Arrays.fill(value, (byte) 1);
        return value;
    }
}

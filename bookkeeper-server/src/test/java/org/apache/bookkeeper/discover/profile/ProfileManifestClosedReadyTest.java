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
import org.apache.bookkeeper.discover.profile.ProfileClosedReconstruction.Member;
import org.apache.bookkeeper.discover.profile.ProfileManifestClosedAdmission.ArenaEpoch;
import org.apache.bookkeeper.discover.profile.ProfileManifestClosedAdmission.ControlPoint;
import org.apache.bookkeeper.discover.profile.ProfileManifestClosedAdmission.Ledger;
import org.junit.Test;

public class ProfileManifestClosedReadyTest {
    private static final String SHA = "2d".repeat(32);
    private static final UUID OPERATION = new UUID(0, 40);
    private static final UUID NONCE = new UUID(0, 41);

    @Test
    public void largestCanonicalReadyVectorHasTheExactNativeSize() throws Exception {
        var candidates = new ArrayList<ProfileManifestClosedReady.Arena>();
        for (int arena = 0; arena < 64; arena++) {
            long device = arena + 1L;
            var files = new ArrayList<FileImage>();
            for (int file = 0; file < 64; file++) {
                files.add(file(file, device, file + 11));
            }
            candidates.add(new ProfileManifestClosedReady.Arena(new FileIdentity(device, 10), files));
        }
        var ready = ready(candidates);
        byte[] nativeBytes = ready.encode();
        assertEquals(234669, nativeBytes.length);
        assertEquals(ready, ProfileManifestClosedReady.decode(nativeBytes));
        assertArrayEquals(nativeBytes, ProfileManifestClosedReady.decode(nativeBytes).encode());
        byte[] unknown = nativeBytes.clone();
        ByteBuffer.wrap(unknown).putInt(4, 2);
        assertThrows(IOException.class, () -> ProfileManifestClosedReady.decode(unknown));
        assertThrows(IOException.class, () -> ProfileManifestClosedReady.decode(
                Arrays.copyOf(nativeBytes, nativeBytes.length - 1)));
        assertThrows(IOException.class, () -> ProfileManifestClosedReady.decode(
                Arrays.copyOf(nativeBytes, nativeBytes.length + 1)));
        assertThrows(IOException.class, () -> ProfileManifestClosedReady.decode(
                new byte[ProfileManifestClosedReady.MAX_BYTES + 1]));
    }

    @Test
    public void canonicalStageMustAlsoMatchEveryOriginalFileAndAvoidItsObjects() throws Exception {
        var scope = new ProfileManifestClosedAdmission.Scope("bookie:3181",
                new StorageIncarnation(bytes(16)), new ManifestIdentity(bytes(32)),
                List.of(new ArenaEpoch(new UUID(0, 1), 2)));
        var originalFile = file(0, 1, 3);
        var arena = new ProfileManifestClosedAdmission.Arena(point(2), true, HistoryMode.ORIGINAL,
                List.of(originalFile));
        var ledger = new ProfileLedgerContext(1, bytes(16), bytes(36));
        var admission = new ProfileManifestClosedAdmission(scope, OPERATION, 1, 1, 1, point(1),
                List.of(arena), List.of(new Ledger(new Member(1, ledger, SHA, SHA), 0, 0, 0, 1)));
        var admitted = new ProfileManifestClosedAdmission.Receipt(admission, 11, SHA);
        var stage = new FileIdentity(1, 4);
        var fresh = file(0, 1, 5);
        var valid = new ProfileManifestClosedReady.Arena(stage, List.of(fresh));
        ready(List.of(valid)).requireAdmission(admitted);
        assertThrows(IllegalArgumentException.class, () -> ready(List.of(valid, valid)));
        assertThrows(IllegalArgumentException.class,
                () -> new ProfileManifestClosedReady.Arena(stage, List.of(file(1, 1, 5))));
        assertThrows(IllegalArgumentException.class,
                () -> new ProfileManifestClosedReady.Arena(stage, List.of(file(0, 2, 5))));
        assertThrows(IOException.class,
                () -> ready(List.of(new ProfileManifestClosedReady.Arena(stage, List.of(originalFile))))
                        .requireAdmission(admitted));
        var changed = new FileImage(0, new Image(fresh.image().object(), 8192, SHA));
        assertThrows(IOException.class,
                () -> ready(List.of(new ProfileManifestClosedReady.Arena(stage, List.of(changed))))
                        .requireAdmission(admitted));
    }

    private static ProfileManifestClosedReady ready(List<ProfileManifestClosedReady.Arena> candidates) {
        return new ProfileManifestClosedReady(11, SHA, OPERATION, 12, 0, NONCE, NONCE, candidates);
    }

    private static FileImage file(int index, long device, long inode) {
        return new FileImage(index, new Image(new FileIdentity(device, inode), 4096, SHA));
    }

    private static ControlPoint point(long inode) {
        return new ControlPoint(new FileIdentity(1, inode), 2, 3, 1, SHA);
    }

    private static byte[] bytes(int count) {
        var bytes = new byte[count];
        Arrays.fill(bytes, (byte) 1);
        return bytes;
    }
}

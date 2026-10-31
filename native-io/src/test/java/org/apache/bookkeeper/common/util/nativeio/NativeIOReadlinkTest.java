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
package org.apache.bookkeeper.common.util.nativeio;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HexFormat;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Original ordinary-link observations only, never Linux deployment qualification. */
public class NativeIOReadlinkTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    private Path root() throws Exception {
        Path root = temporary.getRoot().toPath().toRealPath();
        Files.setPosixFilePermissions(root, PosixFilePermissions.fromString("rwx------"));
        return root;
    }

    private static NativeIO nativeIo() {
        assumeTrue(SystemUtils.IS_OS_LINUX || NativeIOLibraryPath.configuredLibraryPath() != null);
        return new NativeIOImpl();
    }

    @Test
    public void returnsOriginalDanglingTargetBytesAndNoFollowIdentityWithoutChangingTheLink() throws Exception {
        NativeIO io = nativeIo();
        String target = "../不存在-\uD834\uDD1E";
        Path link = Files.createSymbolicLink(root().resolve("link"), Path.of(target));
        String fields = "unix:dev,ino,uid,gid,mode,nlink,size,lastModifiedTime,ctime";
        var before = Files.readAttributes(link, fields, LinkOption.NOFOLLOW_LINKS);
        var observed = io.observeSymbolicLink(link.toString());
        assertArrayEquals(target.getBytes(StandardCharsets.UTF_8), observed.target());
        assertEquals(((Number) before.get("ino")).longValue(), observed.inode());
        assertEquals(((Number) before.get("dev")).longValue(), observed.device());
        assertEquals(before.get("mode"), observed.mode());
        assertEquals(before, Files.readAttributes(link, fields, LinkOption.NOFOLLOW_LINKS));
        byte[] changed = observed.target();
        changed[0] = 0;
        assertArrayEquals(target.getBytes(StandardCharsets.UTF_8), observed.target());
        assertThrows(NativeIOException.class, () -> io.hasExtendedAcl(link.toString()));
    }

    @Test
    public void missingRegularDirectoryDeviceAndBadPathsAreErrorsInsteadOfEmptyTargets() throws Exception {
        NativeIO io = nativeIo();
        Path root = root();
        Path file = Files.write(root.resolve("file"), new byte[]{7});
        for (String path : new String[]{root.resolve("missing").toString(), file.toString(), root.toString(),
                "/dev/null", "relative", null, file + "\0suffix", root + "/\uD800"}) {
            assertTrue(assertThrows(NativeIOException.class, () -> io.observeSymbolicLink(path)).getErrno() > 0);
        }
        assertArrayEquals(new byte[]{7}, Files.readAllBytes(file));
    }

    @Test
    public void resultCopiesRawBytesAndRejectsInvalidBoundariesAndIdentity() {
        byte[] raw = {(byte) 0xff, (byte) 0xc0};
        var value = observation(raw, raw.length);
        raw[0] = 0;
        assertArrayEquals(new byte[]{(byte) 0xff, (byte) 0xc0}, value.target());
        assertEquals(4096, observation(new byte[4096], 4096).target().length);
        assertThrows(IllegalArgumentException.class, () -> observation(new byte[4097], 4097));
        assertThrows(IllegalArgumentException.class, () -> observation(new byte[0], 0));
        assertThrows(IllegalArgumentException.class, () -> observation(new byte[3], 2));
    }

    private static NativeIO.SymbolicLinkObservation observation(byte[] target, long size) {
        return new NativeIO.SymbolicLinkObservation(1, 2, 0, 0, 0120777, 1, size, 0, 0, 0, 0, target);
    }

    @Test
    public void procMagicLinksAreNotOrdinaryAliases() throws Exception {
        assumeTrue(SystemUtils.IS_OS_LINUX);
        assertThrows(NativeIOException.class, () -> nativeIo().observeSymbolicLink("/proc/self/exe"));
    }

    /** Run in a separate JVM with the exact preceding JNI; no alternative loader or capability fallback. */
    public static final class OldLibraryProbe {
        public static void main(String[] args) throws Exception {
            NativeIO io = new NativeIOImpl();
            int fd = io.open(args[0], NativeIO.O_RDONLY, 0);
            assertEquals(0, io.close(fd));
            var failure = assertThrows(NativeIOException.class, () -> io.observeSymbolicLink(args[1]));
            assertTrue(failure.getCause() instanceof LinkageError);
        }
    }

    /** Actual external raw-byte fixture probe: never follows the supplied target. */
    public static final class RawTargetProbe {
        public static void main(String[] args) throws Exception {
            var value = new NativeIOImpl().observeSymbolicLink(args[0]);
            System.out.println(HexFormat.of().formatHex(value.target()));
        }
    }
}

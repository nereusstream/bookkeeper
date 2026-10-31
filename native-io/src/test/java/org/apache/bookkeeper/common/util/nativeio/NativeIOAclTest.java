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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Actual host ACL queries only. The native predicate does not establish Unix or startup authority. */
public class NativeIOAclTest {
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
    public void plainFilesAndDirectoriesHaveNoExtraAclAndQueriesPreserveIdentityModeAndBytes() throws Exception {
        NativeIO io = nativeIo();
        Path root = root();
        Path file = Files.write(root.resolve("公钥-\uD834\uDD1E"), new byte[]{7});
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-------"));
        for (Path member : new Path[]{root, file}) {
            var before = Files.readAttributes(member, "unix:dev,ino,mode,ctime", LinkOption.NOFOLLOW_LINKS);
            assertFalse(io.hasExtendedAcl(member.toString()));
            assertEquals(before, Files.readAttributes(member, "unix:dev,ino,mode,ctime", LinkOption.NOFOLLOW_LINKS));
        }
        assertArrayEquals(new byte[]{7}, Files.readAllBytes(file));
    }

    @Test
    public void missingSpecialAndSymbolicPathsAreErrorsInsteadOfAbsentAcls() throws Exception {
        NativeIO io = nativeIo();
        Path file = Files.write(root().resolve("file"), new byte[]{7});
        Path link = Files.createSymbolicLink(root().resolve("link"), file);
        Path dangling = Files.createSymbolicLink(root().resolve("dangling"), root().resolve("missing"));
        for (String path : new String[]{root().resolve("missing").toString(), "/dev/null",
                link.toString(), dangling.toString()}) {
            assertTrue(assertThrows(NativeIOException.class, () -> io.hasExtendedAcl(path)).getErrno() > 0);
        }
        assertTrue(Files.isSymbolicLink(link));
        assertTrue(Files.isSymbolicLink(dangling));
        assertArrayEquals(new byte[]{7}, Files.readAllBytes(file));
    }

    @Test
    public void nullNulAndInvalidUtf16CannotQueryADifferentPath() throws Exception {
        NativeIO io = nativeIo();
        Path file = Files.write(root().resolve("file"), new byte[]{7});
        for (String path : new String[]{null, file + "\0suffix", root() + "/\uD800", root() + "/\uDC00"}) {
            assertEquals(22, assertThrows(NativeIOException.class, () -> io.hasExtendedAcl(path)).getErrno());
        }
        assertArrayEquals(new byte[]{7}, Files.readAllBytes(file));
    }

    @Test
    public void actualMacAclsAreDetectedEvenWhenPosixModeStillLooksPrivate() throws Exception {
        assumeTrue(SystemUtils.IS_OS_MAC_OSX);
        NativeIO io = nativeIo();
        for (boolean directory : new boolean[]{false, true}) {
            Path member = directory ? Files.createDirectory(root().resolve("acl-directory"))
                    : Files.write(root().resolve("acl-file"), new byte[]{7});
            var mode = PosixFilePermissions.fromString(directory ? "rwx------" : "rw-------");
            Files.setPosixFilePermissions(member, mode);
            assertFalse(io.hasExtendedAcl(member.toString()));
            Process process = new ProcessBuilder("/bin/chmod", "+a", directory
                    ? "everyone allow delete_child" : "everyone allow write", member.toString())
                    .redirectErrorStream(true).start();
            try {
                assertTrue(process.waitFor(5, TimeUnit.SECONDS));
                assertEquals(new String(process.getInputStream().readAllBytes(),
                        java.nio.charset.StandardCharsets.UTF_8),
                        0, process.exitValue());
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                    assertTrue(process.waitFor(5, TimeUnit.SECONDS));
                }
                process.getInputStream().close();
                process.getOutputStream().close();
                process.getErrorStream().close();
            }
            assertEquals(mode, Files.getPosixFilePermissions(member));
            var before = Files.readAttributes(member, "unix:dev,ino,mode,ctime", LinkOption.NOFOLLOW_LINKS);
            assertTrue(io.hasExtendedAcl(member.toString()));
            assertEquals(before, Files.readAttributes(member, "unix:dev,ino,mode,ctime", LinkOption.NOFOLLOW_LINKS));
        }
    }

    /** Run with the exact preceding JNI in a separate JVM, preserving normal native open/close. */
    public static final class OldLibraryProbe {
        public static void main(String[] args) throws Exception {
            NativeIO io = new NativeIOImpl();
            int fd = io.open(args[0], NativeIO.O_RDONLY, 0);
            assertTrue(fd >= 0);
            assertEquals(0, io.close(fd));
            var failure = assertThrows(NativeIOException.class, () -> io.hasExtendedAcl(args[0]));
            assertTrue(failure.getCause() instanceof UnsatisfiedLinkError);
            System.out.println("OLD_JNI_ACL_UNSUPPORTED");
        }
    }
}

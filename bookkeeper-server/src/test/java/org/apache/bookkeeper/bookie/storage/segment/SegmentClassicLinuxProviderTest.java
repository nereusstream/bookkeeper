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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/** Raw text and actual host path byte comparisons only. This fixture cannot bind a Linux production provider. */
public class SegmentClassicLinuxProviderTest {
    static long deadline() {
        return System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
    }

    static byte[] replaceFirstText(byte[] bytes, String original, String replacement) throws IOException {
        byte[] before = original.getBytes(StandardCharsets.UTF_8);
        byte[] after = replacement.getBytes(StandardCharsets.UTF_8);
        for (int i = 16; i + before.length + 2 <= bytes.length; i++) {
            if (Short.toUnsignedInt(ByteBuffer.wrap(bytes).getShort(i)) == before.length
                    && Arrays.equals(Arrays.copyOfRange(bytes, i + 2, i + 2 + before.length), before)) {
                var result = new ByteArrayOutputStream();
                result.write(bytes, 0, i);
                result.write(ByteBuffer.allocate(2).putShort((short) after.length).array());
                result.write(after);
                result.write(bytes, i + before.length + 2, bytes.length - i - before.length - 2);
                byte[] encoded = result.toByteArray();
                ByteBuffer.wrap(encoded).putInt(12, encoded.length - 16);
                return encoded;
            }
        }
        throw new IOException("Original independent vector text is missing");
    }

    static SegmentClassicLauncherInputs schemaTwoInput() throws IOException {
        var original = SegmentClassicLauncherInputs.decode(
                SegmentClassicLinuxDeploymentTest.vector("classic-launcher-inputs-v2"));
        var deployment = SegmentClassicLinuxDeployment.decode(
                SegmentClassicLinuxDeploymentTest.vector("classic-linux-deployment-v2"));
        var child = SegmentClassicLaunchProgram.identity(
                SegmentClassicLinuxDeploymentTest.vector("classic-linux-program-v1"));
        var dropper = deployment.versionTwo().files().get(deployment.versionTwo().isolation().dropper());
        return new SegmentClassicLauncherInputs(original.recipeId(), original.recipe(), original.signerKeyId(),
                SegmentClassicLauncherInputs.parentProgramIdentity(child, deployment), original.os(),
                original.architecture(), new SegmentClassicLaunchProgram.File(Path.of(dropper.path()),
                        SegmentClassicLaunchProgram.NATIVE, dropper.length(), dropper.sha256()),
                original.supervisorDirectory(), original.targetUid(), original.manifest(), original.proof(),
                original.policy(), original.aesKey(), deployment);
    }

    @Test
    public void componentComparisonRetainsOriginalLogicalDirectoryAndAliasSemantics() throws Exception {
        for (String raw : List.of("/", "//./../", "../雪//./%#?/../tail/", "/literal%2F/雪?#", "./relative/")) {
            assertSame(raw, SegmentClassicLaunchTrust.requireUtf8LoadingText(raw, deadline()));
        }
    }

    @Test
    public void invalidTextAndExpiredDeadlineRejectBeforePathUse() {
        for (String raw : List.of("", "nul\0name", "/" + (char) 0xd800, "a".repeat(4097), "雪".repeat(1366))) {
            assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.requireUtf8LoadingText(raw, deadline()));
        }
        assertThrows(IOException.class, () -> SegmentClassicLaunchTrust.requireUtf8LoadingText(null, deadline()));
        assertThrows(IOException.class,
                () -> SegmentClassicLaunchTrust.requireUtf8LoadingText("/literal", System.nanoTime()));
    }

    @Test
    public void standaloneDecodedDataRetainsAllSixOuterTextsAndDoesNotMintSource() throws Exception {
        var input = schemaTwoInput();
        byte[] original = input.encode();
        byte[] changed = replaceFirstText(original, input.dropper().path().toString(), "/usr//libexec/bk-dropper");
        var decoded = SegmentClassicLauncherInputs.decodeWithRawPaths(changed);
        assertEquals("/usr//libexec/bk-dropper", decoded.paths().dropper());
        assertEquals(input, decoded.value());
        assertEquals(List.of("/usr//libexec/bk-dropper", input.supervisorDirectory().toString(),
                input.manifest().toString(), input.proof().toString(), input.policy().toString(),
                input.aesKey().toString()),
                decoded.paths().values());
    }
    static volatile boolean tripwire;
    static int candidatePaths;

    /** Actual JDK default-provider SPI wrapper. The property will be cleared after initialization. */
    public static final class WrapperProvider extends java.nio.file.spi.FileSystemProvider {
        private final java.nio.file.spi.FileSystemProvider delegate;
        private final java.nio.file.FileSystem filesystem;

        public WrapperProvider(java.nio.file.spi.FileSystemProvider delegate) {
            this.delegate = delegate;
            filesystem = new WrappedFileSystem(this, delegate.getFileSystem(java.net.URI.create("file:///")));
        }

        @Override
        public String getScheme() {
            return delegate.getScheme();
        }

        @Override
        public java.nio.file.FileSystem newFileSystem(java.net.URI uri, java.util.Map<String, ?> env)
                throws IOException {
            return delegate.newFileSystem(uri, env);
        }

        @Override
        public java.nio.file.FileSystem getFileSystem(java.net.URI uri) {
            return filesystem;
        }

        @Override
        public Path getPath(java.net.URI uri) {
            return delegate.getPath(uri);
        }

        @Override
        public java.nio.channels.SeekableByteChannel newByteChannel(Path path,
                java.util.Set<? extends java.nio.file.OpenOption> options,
                java.nio.file.attribute.FileAttribute<?>... attrs) throws IOException {
            return delegate.newByteChannel(path, options, attrs);
        }

        @Override
        public java.nio.file.DirectoryStream<Path> newDirectoryStream(Path path,
                java.nio.file.DirectoryStream.Filter<? super Path> filter) throws IOException {
            return delegate.newDirectoryStream(path, filter);
        }

        @Override
        public void createDirectory(Path path, java.nio.file.attribute.FileAttribute<?>... attrs) throws IOException {
            delegate.createDirectory(path, attrs);
        }

        @Override
        public void delete(Path path) throws IOException {
            delegate.delete(path);
        }

        @Override
        public void copy(Path source, Path target, java.nio.file.CopyOption... options) throws IOException {
            delegate.copy(source, target, options);
        }

        @Override
        public void move(Path source, Path target, java.nio.file.CopyOption... options) throws IOException {
            delegate.move(source, target, options);
        }

        @Override
        public boolean isSameFile(Path first, Path second) throws IOException {
            return delegate.isSameFile(first, second);
        }

        @Override
        public boolean isHidden(Path path) throws IOException {
            return delegate.isHidden(path);
        }

        @Override
        public java.nio.file.FileStore getFileStore(Path path) throws IOException {
            return delegate.getFileStore(path);
        }

        @Override
        public void checkAccess(Path path, java.nio.file.AccessMode... modes) throws IOException {
            delegate.checkAccess(path, modes);
        }

        @Override
        public <V extends java.nio.file.attribute.FileAttributeView> V getFileAttributeView(Path path, Class<V> type,
                java.nio.file.LinkOption... options) {
            return delegate.getFileAttributeView(path, type, options);
        }

        @Override
        public <T extends java.nio.file.attribute.BasicFileAttributes> T readAttributes(Path path, Class<T> type,
                java.nio.file.LinkOption... options) throws IOException {
            return delegate.readAttributes(path, type, options);
        }

        @Override
        public java.util.Map<String, Object> readAttributes(Path path, String attributes,
                java.nio.file.LinkOption... options) throws IOException {
            return delegate.readAttributes(path, attributes, options);
        }

        @Override
        public void setAttribute(Path path, String name, Object value, java.nio.file.LinkOption... options)
                throws IOException {
            delegate.setAttribute(path, name, value, options);
        }
    }

    private static final class WrappedFileSystem extends java.nio.file.FileSystem {
        private final WrapperProvider provider;
        private final java.nio.file.FileSystem delegate;

        private WrappedFileSystem(WrapperProvider provider, java.nio.file.FileSystem delegate) {
            this.provider = provider;
            this.delegate = delegate;
        }

        @Override
        public java.nio.file.spi.FileSystemProvider provider() {
            return provider;
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException("Default fixture filesystem cannot be closed");
        }

        @Override
        public boolean isOpen() {
            return delegate.isOpen();
        }

        @Override
        public boolean isReadOnly() {
            return delegate.isReadOnly();
        }

        @Override
        public String getSeparator() {
            return delegate.getSeparator();
        }

        @Override
        public Iterable<Path> getRootDirectories() {
            return delegate.getRootDirectories();
        }

        @Override
        public Iterable<java.nio.file.FileStore> getFileStores() {
            return delegate.getFileStores();
        }

        @Override
        public java.util.Set<String> supportedFileAttributeViews() {
            return delegate.supportedFileAttributeViews();
        }

        @Override
        public Path getPath(String first, String... more) {
            if (tripwire) {
                candidatePaths++;
                throw new AssertionError("Candidate Path constructed before actual default implementation check");
            }
            return delegate.getPath(first, more);
        }

        @Override
        public java.nio.file.PathMatcher getPathMatcher(String syntaxAndPattern) {
            return delegate.getPathMatcher(syntaxAndPattern);
        }

        @Override
        public java.nio.file.attribute.UserPrincipalLookupService getUserPrincipalLookupService() {
            return delegate.getUserPrincipalLookupService();
        }

        @Override
        public java.nio.file.WatchService newWatchService() throws IOException {
            return delegate.newWatchService();
        }
    }

}

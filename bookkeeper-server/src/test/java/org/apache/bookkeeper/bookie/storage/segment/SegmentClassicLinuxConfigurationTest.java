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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Independent bytes and actual file reads only; no ldconfig execution, include expansion or qualification. */
public class SegmentClassicLinuxConfigurationTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    static byte[] vector() throws IOException {
        return SegmentClassicLinuxDeploymentTest.vector("classic-linux-conf-v1");
    }

    private SegmentClassicLinuxConfiguration.Image read(byte[] data) throws IOException {
        Path file = temporary.newFile().toPath();
        Files.write(file, data);
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            channel.position(17);
            var image = SegmentClassicLinuxConfiguration.read(channel, data.length,
                    SegmentClassicLinuxElfTest.deadline());
            assertEquals(17, channel.position());
            assertTrue(channel.isOpen());
            assertArrayEquals(data, image.bytes());
            return image;
        }
    }

    private static List<SegmentClassicLinuxConfiguration.Directive> directives(
            SegmentClassicLinuxConfiguration.Image image) throws IOException {
        var result = new ArrayList<SegmentClassicLinuxConfiguration.Directive>();
        var cursor = image.cursor(SegmentClassicLinuxElfTest.deadline());
        for (var next = cursor.next(); next != null; next = cursor.next()) {
            result.add(next);
        }
        return result;
    }

    @Test
    public void independentVectorPreservesCWhitespaceCaseRawLinesAndEveryIncludeWord() throws Exception {
        byte[] bytes = vector();
        var image = read(bytes);
        var entries = directives(image);
        assertEquals(242, bytes.length);
        assertEquals(13, entries.size());
        var values = new ArrayList<String>();
        for (var entry : entries) {
            values.add(entry.kind() == SegmentClassicLinuxConfiguration.Kind.IGNORED_HWCAP
                    ? "ignored raw hwcap" : entry.value().utf8());
        }
        assertEquals(List.of("/lib", "one.conf", "two.conf", "../parts/[a-z]?.conf", "INCLUDE upper.conf", "include",
                "ignored raw hwcap", "relative/../lib", "crlf.conf\r", "\"a", "b\"", "a\\", "b"), values);
        assertEquals(List.of(2, 3, 3, 3, 4, 5, 6, 8, 9, 10, 10, 10, 10),
                entries.stream().map(SegmentClassicLinuxConfiguration.Directive::line).toList());
        assertEquals(List.of(0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 1, 2, 3),
                entries.stream().map(SegmentClassicLinuxConfiguration.Directive::word).toList());
        assertEquals(3, entries.get(0).column());
        assertEquals(10, entries.get(1).column());
        assertEquals(20, entries.get(2).column());
        assertEquals("\t /lib/// \r\n", entries.get(0).sourceLine().utf8());
        assertThrows(IOException.class, () -> entries.get(6).value().utf8());
        assertEquals(SegmentClassicLinuxConfiguration.Kind.DIRECTORY, entries.get(4).kind());
        assertEquals(SegmentClassicLinuxConfiguration.Kind.INCLUDE, entries.get(8).kind());
        byte[] copy = image.bytes();
        copy[0] = 0;
        byte[] token = entries.get(0).value().bytes();
        token[0] = 0;
        assertArrayEquals(bytes, image.bytes());
        assertEquals("/lib", entries.get(0).value().utf8());
        assertEquals(entries, directives(image));
        assertEquals(image, read(bytes));
        assertEquals(image.hashCode(), read(bytes).hashCode());
        assertFalse(image.equals(read(new byte[0])));
    }

    @Test
    public void noQuotingEscapingExpansionOrUnicodeWhitespaceIsInvented() throws Exception {
        var entries = directives(read(("include\fnot-a-directive\ninclude\tfirst\fsecond\n"
                + "\u00a0/lib///\n~/lib/${HOME}//\n//\n\ufeffinclude x\n/lib/ \t/\n")
                .getBytes(StandardCharsets.UTF_8)));
        assertEquals(6, entries.size());
        assertEquals("include\fnot-a-directive", entries.get(0).value().utf8());
        assertEquals(SegmentClassicLinuxConfiguration.Kind.DIRECTORY, entries.get(0).kind());
        assertEquals("first\fsecond", entries.get(1).value().utf8());
        assertEquals("\u00a0/lib", entries.get(2).value().utf8());
        assertEquals("~/lib/${HOME}", entries.get(3).value().utf8());
        assertEquals("\ufeffinclude x", entries.get(4).value().utf8());
        assertEquals("/lib/ \t", entries.get(5).value().utf8());
    }

    @Test
    public void nulAnywhereIsExplicitlyUnsupportedIncludingCommentsAndIgnoredDirectives() throws Exception {
        for (String content : List.of("include first\0 hidden.conf\n", "/lib\0/ignored\n", "# comment\0\n",
                "hwcap ignored\0\n", "\0")) {
            assertThrows(SegmentClassicLinuxConfiguration.Unsupported.class,
                    () -> read(content.getBytes(StandardCharsets.UTF_8)));
        }
        assertThrows(IOException.class, () -> read(new byte[]{'/', (byte) 255, '\n'}));
        assertThrows(IOException.class, () -> read(new byte[]{'i', 'n', 'c', 'l', 'u', 'd', 'e', ' ', (byte) 255}));
    }

    @Test
    public void contentAndEffectiveTokenBoundsAcceptTheirExactByteLimits() throws Exception {
        byte[] dense = new byte[SegmentClassicLinuxConfiguration.MAX_BYTES];
        for (int i = 0; i < dense.length; i += 2) {
            dense[i] = 'x';
            dense[i + 1] = '\n';
        }
        var image = read(dense);
        int count = 0;
        var cursor = image.cursor(SegmentClassicLinuxElfTest.deadline());
        while (cursor.next() != null) {
            count++;
        }
        assertEquals(524288, count);
        byte[] comment = new byte[dense.length];
        Arrays.fill(comment, (byte) 255);
        comment[0] = '#';
        assertNull(read(comment).cursor(SegmentClassicLinuxElfTest.deadline()).next());
        assertEquals(4096, directives(read("é".repeat(2048).getBytes(StandardCharsets.UTF_8)))
                .get(0).value().bytes().length);
        assertThrows(IOException.class, () -> read("é".repeat(2048).concat("x").getBytes(StandardCharsets.UTF_8)));
        assertNull(read(new byte[0]).cursor(SegmentClassicLinuxElfTest.deadline()).next());
    }

    @Test
    public void rejectedLengthsAndElapsedDeadlinesDoNotPerformAChannelOperation() throws Exception {
        assertThrows(IOException.class, () -> SegmentClassicLinuxConfiguration.read(null, -1,
                SegmentClassicLinuxElfTest.deadline()));
        assertThrows(IOException.class, () -> SegmentClassicLinuxConfiguration.read(null,
                SegmentClassicLinuxConfiguration.MAX_BYTES + 1, SegmentClassicLinuxElfTest.deadline()));
        assertThrows(IOException.class, () -> SegmentClassicLinuxConfiguration.read(null, 0, System.nanoTime() - 1));
        var image = read(new byte[0]);
        assertThrows(IOException.class, () -> image.cursor(System.nanoTime() - 1).next());
    }

    @Test
    public void shortOrZeroProgressAndChangingLengthsAreErrorsWithoutOwningTheChannel() throws Exception {
        Path file = temporary.newFile().toPath();
        Files.write(file, new byte[]{'#', 'x'});
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            assertThrows(IOException.class, () -> SegmentClassicLinuxConfiguration.read(channel, 1,
                    SegmentClassicLinuxElfTest.deadline()));
            for (int result : List.of(0, -1)) {
                FileChannel stalled = spy(channel);
                doReturn(result).when(stalled).read(any(ByteBuffer.class), anyLong());
                assertThrows(IOException.class, () -> SegmentClassicLinuxConfiguration.read(stalled, 2,
                        SegmentClassicLinuxElfTest.deadline()));
                assertTrue(channel.isOpen());
            }
            FileChannel changing = spy(channel);
            doReturn(2L, 3L).when(changing).size();
            assertThrows(IOException.class, () -> SegmentClassicLinuxConfiguration.read(changing, 2,
                    SegmentClassicLinuxElfTest.deadline()));
            assertTrue(channel.isOpen());
            assertEquals(0, channel.position());
        }
    }
}

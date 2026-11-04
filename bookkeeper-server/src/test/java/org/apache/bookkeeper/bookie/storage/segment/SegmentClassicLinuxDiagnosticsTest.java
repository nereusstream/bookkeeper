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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

/** Independent supplied-format data only; no loader invocation, native observation or vendor qualification. */
public class SegmentClassicLinuxDiagnosticsTest {
    private static final List<String> SCALARS = List.of(
            "version.version=\"2.39\"", "dl_dst_lib=\"lib64\"", "dl_hwcap=0xffffffffffffffff",
            "dl_hwcap_important=0x6", "dl_hwcap2=0x8000000000000000",
            "dl_hwcaps_subdirs=\"x86-64-v4:x86-64-v3:x86-64-v2\"", "dl_hwcaps_subdirs_active=0x5",
            "dl_platform=0x0", "dl_string_platform=0xffffffffffffffff", "x86.cpu_features.isa_1=0xffffffff");
    private static final String DIRECTORIES = "path.system_dirs[0x0]=\"/z/../lib//\"\n"
            + "path.system_dirs[0x1]=\"/a/alias/\"\n";

    private static String fixture() {
        return String.join("\n", SCALARS) + "\n" + DIRECTORIES;
    }

    private static byte[] ascii(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }

    private static SegmentClassicLinuxDiagnostics.Parsed parse(String text) throws IOException {
        return SegmentClassicLinuxDiagnostics.parse(ascii(text), SegmentClassicLinuxElfTest.deadline());
    }

    @Test
    public void retainsPrintedBitPatternsIndexOrderAndPrivateRawBytesWithoutCreatingCapabilities() throws Exception {
        byte[] supplied = ascii(fixture());
        var parsed = SegmentClassicLinuxDiagnostics.parse(supplied, SegmentClassicLinuxElfTest.deadline());
        var fields = parsed.fields();
        assertEquals("2.39", fields.version());
        assertEquals("lib64", fields.dstLib());
        assertEquals(-1L, fields.hwcap());
        assertEquals(6, fields.hwcapImportant());
        assertEquals(Long.MIN_VALUE, fields.hwcap2());
        assertEquals("x86-64-v4:x86-64-v3:x86-64-v2", fields.hwcapsSubdirs());
        assertEquals(5, fields.hwcapsSubdirsActive());
        assertEquals(Optional.empty(), fields.platform());
        assertEquals(-1L, fields.stringPlatform());
        assertEquals(0xffffffffL, fields.isa1());
        assertEquals(List.of("/z/../lib//", "/a/alias/"), fields.systemDirectories());
        assertThrows(UnsupportedOperationException.class, () -> fields.systemDirectories().clear());
        supplied[0] = 0;
        byte[] returned = parsed.raw();
        assertArrayEquals(ascii(fixture()), returned);
        returned[0] = 0;
        assertArrayEquals(ascii(fixture()), parsed.raw());
    }

    @Test
    public void distinguishesExplicitNullEmptyAndReversiblyEscapedPlatformStrings() throws Exception {
        assertEquals(Optional.of(""), parse(fixture().replace("dl_platform=0x0", "dl_platform=\"\""))
                .fields().platform());
        assertEquals(Optional.of("haswell"), parse(fixture().replace("dl_platform=0x0", "dl_platform=\"haswell\""))
                .fields().platform());
        String escaped = fixture().replace("dl_platform=0x0", "dl_platform=\"a\\\\b\\\"c\"")
                .replace("/a/alias/", "/a\\\\b\\\"c/");
        var fields = parse(escaped).fields();
        assertEquals(Optional.of("a\\b\"c"), fields.platform());
        assertEquals("/a\\b\"c/", fields.systemDirectories().get(1));
        assertThrows(IOException.class, () -> parse(fixture().replace("dl_platform=0x0", "dl_platform=0x1")));
        assertThrows(IOException.class, () -> parse(fixture().replace("dl_platform=0x0\n", "")));
    }

    @Test
    public void preservesOpaqueNumericEscapesAndRejectsTheirIrreversibleUseInConsumedStrings() throws Exception {
        // In the 2.39 emitter, original bytes 0x08 and 0x10 both print as \000. No reverse guess is possible.
        String opaque = "env[0x0]=\"A=\\000\"\nenv[0x1]=\"B=\\000\"\n"
                + "uname.nodename=\"\\660\\777\\377\"\nx86.cpu_features.features[0xa].cpuid[0x3]=0x0\n";
        assertArrayEquals(ascii(fixture() + opaque), parse(fixture() + opaque).raw());
        for (String original : List.of("\"2.39\"", "\"lib64\"", "\"x86-64-v4:x86-64-v3:x86-64-v2\"",
                "\"/z/../lib//\"")) {
            IOException failure = assertThrows(IOException.class,
                    () -> parse(fixture().replace(original, "\"\\000\"")));
            assertTrue(failure.getMessage().contains("irreversible"));
        }
        for (String escape : List.of("\\000", "\\660", "\\777")) {
            assertThrows(IOException.class,
                    () -> parse(fixture().replace("dl_platform=0x0", "dl_platform=\"" + escape + "\"")));
        }
    }

    @Test
    public void missingRequiredFieldsWrongTypesAndUnsupportedProfileValuesCannotBeDefaulted() throws Exception {
        for (String line : SCALARS) {
            assertThrows(IOException.class, () -> parse(fixture().replace(line + "\n", "")));
        }
        for (String change : List.of("version.version=\"2.40\"", "dl_hwcap=\"0x0\"", "dl_dst_lib=0x0",
                "dl_hwcaps_subdirs_active=0x8", "dl_hwcaps_subdirs_active=0xffffffffffffffff",
                "x86.cpu_features.isa_1=0x100000000", "dl_hwcaps_subdirs=\"x86-64-v3:x86-64-v2\"")) {
            String key = change.substring(0, change.indexOf('='));
            String original = SCALARS.stream().filter(s -> s.startsWith(key + "=")).findFirst().orElseThrow();
            assertThrows(IOException.class, () -> parse(fixture().replace(original, change)));
        }
        var independent = parse(fixture().replace("x86.cpu_features.isa_1=0xffffffff",
                "x86.cpu_features.isa_1=0x0")).fields();
        assertEquals(5, independent.hwcapsSubdirsActive());
        assertEquals(0, independent.isa1());
    }

    @Test
    public void rejectsMalformedLinesDuplicateFieldsAndNoncanonicalOrOverflowingNumbers() throws Exception {
        for (String extra : List.of("a=0x\n", "a=0x00\n", "a=0x10000000000000000\n", "a=0xF\n",
                "a=1\n", "a=-0x1\n", "a[0x00]=0x0\n", "a[]=0x0\n", ".a=0x0\n", "a..b=0x0\n",
                "a=\"unterminated\n", "a=\"x\\n\"\n", "a=\"\\12\"\n", "a=\"\\888\"\n",
                "a=\"x\"tail\n", "a=0x0", "\n", "a=0x0\r\n", "a=0x0\na=0x0\n", SCALARS.get(0) + "\n")) {
            assertThrows(IOException.class, () -> parse(fixture() + extra));
        }
        assertThrows(IOException.class, () -> parse(fixture().substring(0, fixture().length() - 1)));
        assertThrows(IOException.class, () -> parse(fixture() + "a=\"\0\"\n"));
        byte[] nonAscii = ascii(fixture() + "a=\"x\"\n");
        nonAscii[nonAscii.length - 3] = (byte) 255;
        assertThrows(IOException.class, () -> SegmentClassicLinuxDiagnostics.parse(nonAscii,
                SegmentClassicLinuxElfTest.deadline()));
    }

    @Test
    public void systemDirectoriesKeepRawPrefixesWithContinuousIndicesAndExactStringBounds() throws Exception {
        String scalarText = String.join("\n", SCALARS) + "\n";
        var directories = new StringBuilder();
        for (int i = 0; i < 64; i++) {
            directories.append("path.system_dirs[0x").append(Integer.toHexString(i)).append("]=\"/\"\n");
        }
        assertEquals(64, parse(scalarText + directories).fields().systemDirectories().size());
        assertThrows(IOException.class, () -> parse(scalarText + directories + "path.system_dirs[0x40]=\"/\"\n"));
        for (String broken : List.of("", "path.system_dirs[0x1]=\"/lib/\"\n",
                "path.system_dirs[0x0]=\"/\"\npath.system_dirs[0x2]=\"/\"\n",
                "path.system_dirs[0x0]=\"/\"\npath.system_dirs[0x0]=\"/\"\n",
                "path.system_dirs[0x0]=\"relative/\"\n", "path.system_dirs[0x0]=\"/lib\"\n",
                "path.system_dirs[0x0]=0x0\n")) {
            assertThrows(IOException.class, () -> parse(scalarText + broken));
        }
        String prefix = "/" + "x".repeat(4094) + "/";
        assertEquals(prefix, parse(scalarText + "path.system_dirs[0x0]=\"" + prefix + "\"\n")
                .fields().systemDirectories().get(0));
        assertThrows(IOException.class,
                () -> parse(scalarText + "path.system_dirs[0x0]=\"/" + prefix + "\"\n"));
        assertEquals(4096, parse(fixture().replace("\"lib64\"", "\"" + "x".repeat(4096) + "\""))
                .fields().dstLib().length());
        assertThrows(IOException.class,
                () -> parse(fixture().replace("\"lib64\"", "\"" + "x".repeat(4097) + "\"")));
    }

    @Test
    public void exactStdoutByteLimitAndOriginalDeadlineApplyBeforeExpansion() throws Exception {
        int remaining = SegmentClassicLinuxDiagnostics.MAX_BYTES - ascii(fixture() + "opaque=\"\"\n").length;
        String maximum = fixture() + "opaque=\"" + "p".repeat(remaining) + "\"\n";
        assertEquals(65536, ascii(maximum).length);
        assertArrayEquals(ascii(maximum), parse(maximum).raw());
        assertFalse(parse(maximum).fields().platform().isPresent());
        assertThrows(IOException.class, () -> parse(maximum + "\n"));
        assertThrows(IOException.class, () -> SegmentClassicLinuxDiagnostics.parse(null,
                SegmentClassicLinuxElfTest.deadline()));
        assertThrows(IOException.class, () -> parse(""));
        assertThrows(IOException.class,
                () -> SegmentClassicLinuxDiagnostics.parse(ascii(fixture()), System.nanoTime()));
    }
}

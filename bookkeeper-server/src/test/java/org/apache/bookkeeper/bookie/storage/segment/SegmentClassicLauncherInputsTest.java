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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.junit.Test;

/** Independent fixed bytes and argument construction only; no private key, Process or installed provenance. */
public class SegmentClassicLauncherInputsTest {
    private static byte[] vector(String name) throws IOException {
        try (var input = SegmentClassicLauncherInputsTest.class.getResourceAsStream(
                "/unified-wal-segment/" + name + ".hex")) {
            return HexFormat.of().parseHex(new String(input.readAllBytes(), StandardCharsets.US_ASCII).trim());
        }
    }

    private static SegmentClassicLaunchTrust.Parsed original() throws IOException {
        byte[] bytes = vector("classic-launch-recipe-v1");
        return new SegmentClassicLaunchTrust.Parsed(SegmentClassicLaunchTrust.decode(bytes),
                SegmentClassicHandoff.digest("BookKeeper/ControlledClassicRecipe/v1\0"
                        .getBytes(StandardCharsets.US_ASCII), bytes), null);
    }

    @Test
    public void independentVectorBindsTheRecipeAndBuildsOnlyExplicitNumericExecArguments() throws Exception {
        var inputs = SegmentClassicLauncherInputs.decode(vector("classic-launcher-inputs-v1"));
        var original = original();
        inputs.requireRecipe(original);
        assertEquals("classic-1", inputs.recipeId());
        assertEquals(501, inputs.targetUid());
        assertEquals(Path.of("/var/lib/bookkeeper/supervisor"), inputs.supervisorDirectory());
        var program = SegmentClassicLaunchProgram.decode(vector("classic-launch-program-v1"));
        var command = inputs.childCommand(original, program);
        assertEquals(List.of("/opt/bookkeeper/bin/classic-exec", "--uid", "1001", "--gid", "20", "--groups",
                "20,21", "--"), command.subList(0, 8));
        assertEquals(program.command("classic-1"), command.subList(8, command.size()));
        assertThrows(UnsupportedOperationException.class, () -> command.add("injected"));
        var parent = SegmentClassicLauncherInputs.parentCommand(program, "classic-1");
        assertEquals(SegmentClassicLauncherInputs.MAIN, parent.get(parent.size() - 2));
        assertEquals(program.command("classic-1").subList(0, parent.size() - 2),
                parent.subList(0, parent.size() - 2));
    }

    @Test
    public void descriptorFramingAndTruncationRejectBeforeAnyFileOrKeyConsumption() throws Exception {
        byte[] vector = vector("classic-launcher-inputs-v1");
        assertEquals(426, vector.length);
        for (int length = 0; length < vector.length; length++) {
            byte[] truncated = Arrays.copyOf(vector, length);
            assertThrows(IOException.class, () -> SegmentClassicLauncherInputs.decode(truncated));
        }
        for (int offset : List.of(0, 8, 10, 12)) {
            byte[] corrupted = vector.clone();
            corrupted[offset] ^= 1;
            assertThrows(IOException.class, () -> SegmentClassicLauncherInputs.decode(corrupted));
        }
        byte[] trailing = Arrays.copyOf(vector, vector.length + 1);
        ByteBuffer.wrap(trailing).putInt(12, trailing.length - 16);
        assertThrows(IOException.class, () -> SegmentClassicLauncherInputs.decode(trailing));
        assertThrows(IOException.class, () -> SegmentClassicLauncherInputs.decode(new byte[32769]));
    }

    @Test
    public void differentRecipeAndNoncanonicalPathsCannotSelectOtherSecretsOrScopes() throws Exception {
        var inputs = SegmentClassicLauncherInputs.decode(vector("classic-launcher-inputs-v1"));
        var original = original();
        var recipe = original.recipe();
        var other = new SegmentClassicLaunchTrust.Recipe("other", recipe.deployment(), recipe.keyName(),
                recipe.signerKeyId(), recipe.program(), recipe.configuration(), recipe.originalScope(),
                recipe.oldUid(), recipe.primaryGid(), recipe.groups());
        assertThrows(IOException.class, () -> inputs.requireRecipe(
                new SegmentClassicLaunchTrust.Parsed(other, original.recipeIdentity(), null)));
        assertThrows(IllegalArgumentException.class, () -> new SegmentClassicLauncherInputs(inputs.recipeId(),
                inputs.recipe(), inputs.signerKeyId(), inputs.parentProgram(), inputs.os(), inputs.architecture(),
                inputs.dropper(), Path.of("/var/lib/../supervisor"), inputs.targetUid(), inputs.manifest(),
                inputs.proof(), inputs.policy(), inputs.aesKey()));
        assertThrows(IllegalArgumentException.class, () -> new SegmentClassicLauncherInputs(inputs.recipeId(),
                inputs.recipe(), inputs.signerKeyId(), inputs.parentProgram(), inputs.os(), inputs.architecture(),
                inputs.dropper(), inputs.supervisorDirectory(), inputs.targetUid(), inputs.manifest(),
                inputs.proof(), inputs.proof(), inputs.aesKey()));
        assertTrue(inputs.parentProgram().equals(
                SegmentClassicLauncherInputs.parentProgramIdentity(recipe.program())));
    }

    @Test
    public void independentV2IdentityBindsTheWholeDeploymentWithoutChangingV1Bytes() throws Exception {
        byte[] recipe = SegmentClassicLinuxDeploymentTest.vector("classic-linux-recipe-v1");
        var original = new SegmentClassicLaunchTrust.Parsed(SegmentClassicLaunchTrust.decode(recipe),
                SegmentClassicHandoff.digest("BookKeeper/ControlledClassicRecipe/v1\0"
                        .getBytes(StandardCharsets.US_ASCII), recipe), null);
        byte[] bytes = SegmentClassicLinuxDeploymentTest.vector("classic-launcher-inputs-v2");
        var inputs = SegmentClassicLauncherInputs.decode(bytes);
        inputs.requireRecipe(original);
        assertArrayEquals(bytes, inputs.encode());
        assertEquals(1634, bytes.length);
        assertEquals("68439ec6b45fb6ef7ca34e314bb5e5f996346fe581082784b6e85b7a6a527fe1",
                HexFormat.of().formatHex(inputs.parentProgram().value()));
        var child = SegmentClassicLaunchProgram.decode(SegmentClassicLinuxDeploymentTest.vector(
                "classic-linux-program-v1"));
        inputs.linuxDeployment().requireProgram(child, "classic-1");
        assertThrows(IOException.class, () -> inputs.childCommand(original, child));
        byte[] old = vector("classic-launcher-inputs-v1");
        assertArrayEquals(old, SegmentClassicLauncherInputs.decode(old).encode());
    }

    @Test
    public void v2NeverFallsBackToV1AndChangedRulesInvalidateThePreviousParentApproval() throws Exception {
        byte[] bytes = SegmentClassicLinuxDeploymentTest.vector("classic-launcher-inputs-v2");
        for (int offset : List.of(0, 8, 10, 12)) {
            byte[] corrupt = bytes.clone();
            corrupt[offset] ^= 1;
            assertThrows(IOException.class, () -> SegmentClassicLauncherInputs.decode(corrupt));
        }
        byte[] downgraded = bytes.clone();
        downgraded[7] = '1';
        ByteBuffer.wrap(downgraded).putShort(8, (short) 1);
        assertThrows(IOException.class, () -> SegmentClassicLauncherInputs.decode(downgraded));
        byte[] oversizedV1 = Arrays.copyOf(vector("classic-launcher-inputs-v1"), 32769);
        ByteBuffer.wrap(oversizedV1).putInt(12, oversizedV1.length - 16);
        assertThrows(IOException.class, () -> SegmentClassicLauncherInputs.decode(oversizedV1));
        var input = SegmentClassicLauncherInputs.decode(bytes);
        var d = input.linuxDeployment();
        var i = d.isolation();
        var other = new SegmentClassicLinuxDeployment(d.platform(), d.files(), d.loading(), d.startChain(), d.units(),
                d.launcherUnit(), new SegmentClassicLinuxDeployment.Isolation(i.dropper(), i.dropperAbi(),
                        SegmentClassicHandoff.digest(new byte[]{3}), 1, 1, 1), d.operations());
        var changed = new SegmentClassicLauncherInputs(input.recipeId(), input.recipe(), input.signerKeyId(),
                input.parentProgram(), input.os(), input.architecture(), input.dropper(), input.supervisorDirectory(),
                input.targetUid(), input.manifest(), input.proof(), input.policy(), input.aesKey(), other);
        byte[] recipe = SegmentClassicLinuxDeploymentTest.vector("classic-linux-recipe-v1");
        var parsed = new SegmentClassicLaunchTrust.Parsed(SegmentClassicLaunchTrust.decode(recipe),
                SegmentClassicHandoff.digest("BookKeeper/ControlledClassicRecipe/v1\0"
                        .getBytes(StandardCharsets.US_ASCII), recipe), null);
        assertThrows(IOException.class, () -> changed.requireRecipe(parsed));
    }
}

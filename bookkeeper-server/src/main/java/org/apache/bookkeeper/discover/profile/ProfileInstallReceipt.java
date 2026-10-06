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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import org.apache.bookkeeper.common.profile.EngineProfile;
import org.apache.bookkeeper.common.profile.control.ProfileControlOperation;
import org.apache.bookkeeper.common.profile.control.ProfileControlResult;
import org.apache.bookkeeper.common.profile.control.ProfileControlScope;
import org.apache.bookkeeper.common.profile.wire.ProfileHello;

/** Secret-free bounded install receipt from a request-correlated client and its verified HELLO facts. */
public record ProfileInstallReceipt(ProfileControlScope scope, EngineProfile engine, byte[] capabilityDigest,
                                    long localInstallGeneration) {
    public ProfileInstallReceipt {
        Objects.requireNonNull(scope, "scope");
        if (scope.operation() != ProfileControlOperation.INSTALL || engine != EngineProfile.SEGMENT_WAL_ENGINE
                || capabilityDigest == null || capabilityDigest.length != 32 || localInstallGeneration <= 0) {
            throw new IllegalArgumentException("Invalid Profile install receipt");
        }
        capabilityDigest = capabilityDigest.clone();
    }

    @Override
    public byte[] capabilityDigest() {
        return capabilityDigest.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ProfileInstallReceipt that && scope.equals(that.scope) && engine == that.engine
                && Arrays.equals(capabilityDigest, that.capabilityDigest)
                && localInstallGeneration == that.localInstallGeneration;
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, engine, Arrays.hashCode(capabilityDigest), localInstallGeneration);
    }

    static boolean matches(ProfileControlScope scope, ProfileHello.Server hello, ProfileControlResult result) {
        return hello.activeEngine() == EngineProfile.SEGMENT_WAL_ENGINE.wireValue()
                && hello.bookieId().equals(scope.targetBookie())
                && Arrays.equals(hello.storageIncarnation(), scope.targetStorageIncarnation())
                && result != null && result.operation() == ProfileControlOperation.INSTALL
                && result.ledgerId() == scope.ledgerId()
                && result.operationIdHex().equals(HexFormat.of().formatHex(scope.operationId()))
                && result.authorityGeneration() == scope.authorityGeneration()
                && result.localGeneration() > 0 && result.durable()
                && (result.outcome() == ProfileControlResult.Outcome.APPLIED
                        || result.outcome() == ProfileControlResult.Outcome.ALREADY_APPLIED);
    }

    public static ProfileInstallReceipt from(ProfileControlScope scope, ProfileHello.Server hello,
                                      ProfileControlResult result) {
        if (!matches(scope, hello, result)) {
            throw new IllegalArgumentException("Install result does not match the bound target and request");
        }
        ByteBuffer capabilities = ByteBuffer.allocate(8 + 8 * hello.capabilities().size());
        capabilities.putInt(1).putShort((short) hello.activeEngine()).putShort((short) hello.capabilities().size());
        hello.capabilities().forEach(capability -> capabilities.putInt((int) capability.capabilityId())
                .putShort((short) capability.semanticVersion()).putShort((short) 0));
        try {
            MessageDigest hash = MessageDigest.getInstance("SHA-256");
            hash.update("BookKeeper/ProfileInstallCapabilities/v1\0".getBytes(StandardCharsets.US_ASCII));
            hash.update(capabilities.array());
            return new ProfileInstallReceipt(scope, EngineProfile.SEGMENT_WAL_ENGINE,
                    hash.digest(), result.localGeneration());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Required SHA-256 unavailable", impossible);
        }
    }
}

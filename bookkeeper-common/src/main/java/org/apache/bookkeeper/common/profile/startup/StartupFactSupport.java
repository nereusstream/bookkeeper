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

package org.apache.bookkeeper.common.profile.startup;

import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.DUPLICATE_OR_UNORDERED_VALUE;
import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.EMPTY_COLLECTION;
import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.INVALID_FIXED_LENGTH;
import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.INVALID_TEXT;
import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.NULL_FIELD;
import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.TEXT_TOO_LARGE;
import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.ZERO_IDENTIFIER;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

final class StartupFactSupport {

    private StartupFactSupport() {}

    static <T> T requireNonNull(T value) {
        if (value == null) {
            throw new StartupReadinessValidationException(NULL_FIELD);
        }
        return value;
    }

    static byte[] fixedNonZero(byte[] value, int length) {
        if (value == null) {
            throw new StartupReadinessValidationException(NULL_FIELD);
        }
        if (value.length != length) {
            throw new StartupReadinessValidationException(INVALID_FIXED_LENGTH);
        }
        byte[] copy = value.clone();
        for (byte current : copy) {
            if (current != 0) {
                return copy;
            }
        }
        throw new StartupReadinessValidationException(ZERO_IDENTIFIER);
    }

    static String boundedText(String value, int maxUtf8Bytes) {
        if (value == null) {
            throw new StartupReadinessValidationException(NULL_FIELD);
        }
        if (value.isBlank()) {
            throw new StartupReadinessValidationException(INVALID_TEXT);
        }
        for (int index = 0; index < value.length(); index++) {
            if (Character.isISOControl(value.charAt(index))) {
                throw new StartupReadinessValidationException(INVALID_TEXT);
            }
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > maxUtf8Bytes) {
            throw new StartupReadinessValidationException(TEXT_TOO_LARGE);
        }
        return value;
    }

    static List<Integer> strictlyIncreasingPositiveIds(List<Integer> values, boolean allowEmpty) {
        if (values == null) {
            throw new StartupReadinessValidationException(NULL_FIELD);
        }
        if (!allowEmpty && values.isEmpty()) {
            throw new StartupReadinessValidationException(EMPTY_COLLECTION);
        }
        List<Integer> copy = new ArrayList<>(values.size());
        int previous = 0;
        for (Integer value : values) {
            if (value == null || value <= previous) {
                throw new StartupReadinessValidationException(DUPLICATE_OR_UNORDERED_VALUE);
            }
            copy.add(value);
            previous = value;
        }
        return List.copyOf(copy);
    }
}

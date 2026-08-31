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

package org.apache.bookkeeper.common.profile.control;

import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.INVALID_TEXT;
import static org.apache.bookkeeper.common.profile.control.ProfileControlValidationException.Reason.TEXT_TOO_LARGE;

import java.nio.charset.StandardCharsets;

/** Canonical leaf-X509 identity supplied by an already authenticated Profile TLS connection. */
public record ProfilePrincipal(
        MappingMode mappingMode,
        String trustDomain,
        String issuerCanonical,
        String subjectCanonical) {

    public static final int MAX_COMPONENT_BYTES = 1024;

    public enum MappingMode {
        SUBJECT_UNIQUE_WITHIN_TRUST_SET,
        TRUST_DOMAIN_ISSUER_PLUS_SUBJECT
    }

    public ProfilePrincipal {
        if (mappingMode == null) {
            throw new ProfileControlValidationException(INVALID_TEXT);
        }
        requireBounded(trustDomain);
        requireBounded(issuerCanonical);
        requireBounded(subjectCanonical);
    }

    /** Stable authorization identity without exposing certificate material. */
    public String authorizationIdentity() {
        if (mappingMode == MappingMode.SUBJECT_UNIQUE_WITHIN_TRUST_SET) {
            return subjectCanonical;
        }
        return trustDomain + '/' + issuerCanonical + '/' + subjectCanonical;
    }

    private static void requireBounded(String value) {
        if (value == null || value.isBlank() || value.indexOf('\0') >= 0) {
            throw new ProfileControlValidationException(INVALID_TEXT);
        }
        if (value.getBytes(StandardCharsets.UTF_8).length > MAX_COMPONENT_BYTES) {
            throw new ProfileControlValidationException(TEXT_TOO_LARGE);
        }
    }
}

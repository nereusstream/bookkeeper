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

import java.util.Optional;

/** Transport facts delivered by an isolated listener before any control request is processed. */
public record ProfileTransportContext(
        String endpoint,
        String tlsProtocol,
        boolean immediateTls,
        boolean serverAuthenticated,
        boolean clientCertificateAuthenticated,
        ProfilePrincipal principal) {

    public static final String PROFILE_ENDPOINT = "bookie-profile";
    public static final String REQUIRED_TLS_PROTOCOL = "TLSv1.3";

    public Optional<ProfilePrincipal> authenticatedPrincipal() {
        return Optional.ofNullable(principal);
    }

    public boolean isRequiredMutualTls() {
        return PROFILE_ENDPOINT.equals(endpoint)
                && REQUIRED_TLS_PROTOCOL.equals(tlsProtocol)
                && immediateTls
                && serverAuthenticated
                && clientCertificateAuthenticated
                && principal != null;
    }
}

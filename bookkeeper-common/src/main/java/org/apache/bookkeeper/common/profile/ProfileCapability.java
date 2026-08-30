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

package org.apache.bookkeeper.common.profile;

/** Exact mandatory capability identifier and semantic version. */
public record ProfileCapability(long capabilityId, int semanticVersion) {

    static final long MAX_CAPABILITY_ID = 0xffff_ffffL;
    static final int MAX_SEMANTIC_VERSION = 0xffff;

    public ProfileCapability {
        if (capabilityId <= 0 || capabilityId > MAX_CAPABILITY_ID) {
            throw new IllegalArgumentException("capabilityId must be a non-zero unsigned u32");
        }
        if (semanticVersion <= 0 || semanticVersion > MAX_SEMANTIC_VERSION) {
            throw new IllegalArgumentException("semanticVersion must be a non-zero unsigned u16");
        }
    }
}

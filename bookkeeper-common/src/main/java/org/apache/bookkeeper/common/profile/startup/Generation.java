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

import static org.apache.bookkeeper.common.profile.startup.StartupReadinessValidationException.Reason.INVALID_GENERATION;

/** Strict positive generation used by every authoritative startup fact. */
public record Generation(long value) implements Comparable<Generation> {

    public Generation {
        if (value <= 0) {
            throw new StartupReadinessValidationException(INVALID_GENERATION);
        }
    }

    @Override
    public int compareTo(Generation other) {
        return Long.compare(value, StartupFactSupport.requireNonNull(other).value);
    }
}

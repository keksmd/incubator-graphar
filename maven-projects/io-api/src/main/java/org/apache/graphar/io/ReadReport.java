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

package org.apache.graphar.io;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/** Per-read accounting of physical hints applied or declined by a backend. */
public final class ReadReport {
    private final Set<ReadCapability> applied;
    private final Set<ReadCapability> declined;

    public ReadReport(Set<ReadCapability> applied, Set<ReadCapability> declined) {
        EnumSet<ReadCapability> appliedCopy = copyOf(applied);
        EnumSet<ReadCapability> declinedCopy = copyOf(declined);
        EnumSet<ReadCapability> overlap = EnumSet.copyOf(appliedCopy);
        overlap.retainAll(declinedCopy);
        if (!overlap.isEmpty()) {
            throw new IllegalArgumentException(
                    "A read capability cannot be both applied and declined.");
        }
        this.applied = Collections.unmodifiableSet(appliedCopy);
        this.declined = Collections.unmodifiableSet(declinedCopy);
    }

    /** Returns capabilities physically applied by this read. */
    public Set<ReadCapability> applied() {
        return applied;
    }

    /** Returns requested capabilities handled through semantics-preserving fallback. */
    public Set<ReadCapability> declined() {
        return declined;
    }

    private static EnumSet<ReadCapability> copyOf(Set<ReadCapability> capabilities) {
        Objects.requireNonNull(capabilities, "Read capabilities cannot be null.");
        return capabilities.isEmpty()
                ? EnumSet.noneOf(ReadCapability.class)
                : EnumSet.copyOf(capabilities);
    }
}

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

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/** A cursor and the complete physical-capability accounting for one read operation. */
public final class ReadResult {
    private final BatchCursor cursor;
    private final ReadReport report;

    /**
     * Creates a result whose report completely accounts for every requested physical capability.
     */
    public ReadResult(ReadRequest request, BatchCursor cursor, ReadReport report) {
        Objects.requireNonNull(request, "A read request cannot be null.");
        this.cursor = Objects.requireNonNull(cursor, "A batch cursor cannot be null.");
        this.report = Objects.requireNonNull(report, "A read report cannot be null.");
        EnumSet<ReadCapability> accounted = EnumSet.noneOf(ReadCapability.class);
        accounted.addAll(report.applied());
        accounted.addAll(report.declined());
        Set<ReadCapability> requested = request.requestedCapabilities();
        if (!accounted.equals(requested)) {
            throw new IllegalArgumentException(
                    "A read report must account for each requested capability and no others.");
        }
    }

    /** Returns the batches from this operation. */
    public BatchCursor cursor() {
        return cursor;
    }

    /** Returns which hints were physically applied or declined. */
    public ReadReport report() {
        return report;
    }
}

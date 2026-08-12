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

import java.util.Objects;

/** A half-open physical row range: {@code [startInclusive, endExclusive)}. */
public final class RowRange {
    private final long startInclusive;
    private final long endExclusive;

    public RowRange(long startInclusive, long endExclusive) {
        if (startInclusive < 0 || endExclusive < startInclusive) {
            throw new IllegalArgumentException(
                    "A row range must satisfy 0 <= startInclusive <= endExclusive.");
        }
        this.startInclusive = startInclusive;
        this.endExclusive = endExclusive;
    }

    public long startInclusive() {
        return startInclusive;
    }

    public long endExclusive() {
        return endExclusive;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof RowRange)) {
            return false;
        }
        RowRange that = (RowRange) other;
        return startInclusive == that.startInclusive && endExclusive == that.endExclusive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startInclusive, endExclusive);
    }
}

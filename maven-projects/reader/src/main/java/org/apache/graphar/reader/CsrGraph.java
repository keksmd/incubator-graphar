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

package org.apache.graphar.reader;

/** An immutable heap CSR topology representation with long vertex and destination IDs. */
public final class CsrGraph {
    private final long[] offsets;
    private final long[] destinations;

    CsrGraph(long[] offsets, long[] destinations) {
        if (offsets.length == 0
                || offsets[0] != 0
                || offsets[offsets.length - 1] != destinations.length) {
            throw new IllegalArgumentException(
                    "CSR offsets must start at zero and end at edge count.");
        }
        long previous = 0;
        for (long offset : offsets) {
            if (offset < previous || offset > destinations.length) {
                throw new IllegalArgumentException("CSR offsets must be monotonic and in bounds.");
            }
            previous = offset;
        }
        this.offsets = offsets.clone();
        this.destinations = destinations.clone();
    }

    /** Returns the number of vertices. */
    public long vertexCount() {
        return offsets.length - 1L;
    }

    /** Returns the number of edges. */
    public long edgeCount() {
        return destinations.length;
    }

    /** Returns a defensive copy of the CSR offset array. */
    public long[] offsets() {
        return offsets.clone();
    }

    /** Returns a defensive copy of the CSR destination array. */
    public long[] destinations() {
        return destinations.clone();
    }
}

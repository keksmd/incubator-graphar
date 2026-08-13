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

    /** Returns the number of entries adjacent to {@code vertex}. */
    public long degree(long vertex) {
        int index = checkedIndex(vertex);
        return offsets[index + 1] - offsets[index];
    }

    /**
     * Returns the neighbour stored at {@code position} within the adjacency of {@code vertex}.
     * Serving code iterates a vertex through this accessor and through {@link #degree(long)}, which
     * costs no allocation, instead of exporting the arrays.
     */
    public long neighbor(long vertex, long position) {
        int index = checkedIndex(vertex);
        long from = offsets[index];
        if (position < 0 || position >= offsets[index + 1] - from) {
            throw new IllegalArgumentException(
                    "Adjacency position is outside the neighbourhood of "
                            + vertex
                            + ": "
                            + position);
        }
        return destinations[Math.toIntExact(from + position)];
    }

    /**
     * Returns a defensive copy of the CSR offset array. The copy is the size of the graph, so bulk
     * export is for handing the topology to another component once, not for serving requests.
     */
    public long[] offsets() {
        return offsets.clone();
    }

    /**
     * Returns a defensive copy of the CSR destination array. The copy is the size of the graph, so
     * bulk export is for handing the topology to another component once, not for serving requests.
     */
    public long[] destinations() {
        return destinations.clone();
    }

    long[] rawOffsets() {
        return offsets;
    }

    long[] rawDestinations() {
        return destinations;
    }

    private int checkedIndex(long vertex) {
        if (vertex < 0 || vertex >= vertexCount()) {
            throw new IllegalArgumentException("Vertex is outside the graph: " + vertex);
        }
        return Math.toIntExact(vertex);
    }
}

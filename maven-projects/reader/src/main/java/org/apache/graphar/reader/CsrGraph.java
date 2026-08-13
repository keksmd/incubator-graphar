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

/**
 * An immutable heap CSR topology.
 *
 * <p>The public accessors speak {@code long}, because a vertex identifier is a graph-level concept
 * and callers should not have to know how wide the storage is. The storage itself is {@code int},
 * because {@link ProjectionCapacity} already refuses any graph whose vertex count or entry count
 * does not fit an array index. A projection this class can hold is therefore a projection whose
 * identifiers fit thirty-two bits, and storing them in sixty-four wasted half of the topology.
 */
public final class CsrGraph {
    private final int[] offsets;
    private final int[] destinations;

    CsrGraph(int[] offsets, int[] destinations) {
        if (offsets.length == 0
                || offsets[0] != 0
                || offsets[offsets.length - 1] != destinations.length) {
            throw new IllegalArgumentException(
                    "CSR offsets must start at zero and end at edge count.");
        }
        int previous = 0;
        for (int offset : offsets) {
            if (offset < previous || offset > destinations.length) {
                throw new IllegalArgumentException("CSR offsets must be monotonic and in bounds.");
            }
            previous = offset;
        }
        this.offsets = offsets;
        this.destinations = destinations;
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
        return offsets[index + 1] - (long) offsets[index];
    }

    /**
     * Returns the neighbour stored at {@code position} within the adjacency of {@code vertex}.
     * Serving code iterates a vertex through this accessor and through {@link #degree(long)}, which
     * costs no allocation, instead of exporting the arrays.
     */
    public long neighbor(long vertex, long position) {
        int index = checkedIndex(vertex);
        int from = offsets[index];
        if (position < 0 || position >= offsets[index + 1] - (long) from) {
            throw new IllegalArgumentException(
                    "Adjacency position is outside the neighbourhood of "
                            + vertex
                            + ": "
                            + position);
        }
        return destinations[Math.toIntExact(from + position)];
    }

    /**
     * Returns a widened copy of the CSR offset array. The copy is the size of the graph, so bulk
     * export is for handing the topology to another component once, not for serving requests.
     */
    public long[] offsets() {
        return widen(offsets);
    }

    /**
     * Returns a widened copy of the CSR destination array. The copy is the size of the graph, so
     * bulk export is for handing the topology to another component once, not for serving requests.
     */
    public long[] destinations() {
        return widen(destinations);
    }

    int[] rawOffsets() {
        return offsets;
    }

    int[] rawDestinations() {
        return destinations;
    }

    private static long[] widen(int[] values) {
        long[] widened = new long[values.length];
        for (int index = 0; index < values.length; index++) {
            widened[index] = values[index];
        }
        return widened;
    }

    private int checkedIndex(long vertex) {
        if (vertex < 0 || vertex >= vertexCount()) {
            throw new IllegalArgumentException("Vertex is outside the graph: " + vertex);
        }
        return (int) vertex;
    }
}

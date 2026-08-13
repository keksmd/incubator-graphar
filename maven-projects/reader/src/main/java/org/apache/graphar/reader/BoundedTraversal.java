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

import java.util.Arrays;
import java.util.Objects;

/**
 * Breadth-first expansion over a materialized CSR under an explicit depth and node budget.
 *
 * <p>A serving API cannot expose an unbounded expansion: one hub vertex is enough to pull a whole
 * component into a response. Both entry points therefore stop at a caller-supplied node budget and
 * report that they stopped, so the caller can answer with a truncated result instead of timing out.
 *
 * <p>Results are deterministic for a given CSR: each frontier is expanded in ascending vertex order
 * and newly discovered vertices are appended in ascending order, independent of the order in which
 * the adjacency stores them.
 */
public final class BoundedTraversal {
    private BoundedTraversal() {}

    /** Expands at most {@code maxDepth} hops from {@code start}, stopping at {@code maxNodes}. */
    public static TraversalResult neighborhood(
            CsrGraph csr, long start, int maxDepth, int maxNodes) {
        return expand(csr, start, maxDepth, maxNodes);
    }

    /**
     * Expands the whole connected component of {@code start}, stopping at {@code maxNodes}. The
     * result is a component only when it is not truncated.
     */
    public static TraversalResult component(CsrGraph csr, long start, int maxNodes) {
        return expand(csr, start, Integer.MAX_VALUE, maxNodes);
    }

    private static TraversalResult expand(CsrGraph csr, long start, int maxDepth, int maxNodes) {
        Objects.requireNonNull(csr, "CSR cannot be null.");
        if (maxDepth < 0) {
            throw new IllegalArgumentException("Traversal depth must be non-negative: " + maxDepth);
        }
        if (maxNodes < 1) {
            throw new IllegalArgumentException(
                    "Traversal node budget must be positive: " + maxNodes);
        }
        long vertexCount = csr.vertexCount();
        if (start < 0 || start >= vertexCount) {
            throw new IllegalArgumentException("Start vertex is outside the graph: " + start);
        }
        long[] offsets = csr.offsets();
        long[] destinations = csr.destinations();
        boolean[] discovered = new boolean[Math.toIntExact(vertexCount)];

        int capacity = Math.toIntExact(Math.min(maxNodes, vertexCount));
        long[] vertices = new long[capacity];
        int[] depths = new int[capacity];
        int size = 0;
        vertices[size] = start;
        depths[size] = 0;
        size++;
        discovered[Math.toIntExact(start)] = true;

        boolean truncated = false;
        int frontierStart = 0;
        int frontierEnd = 1;
        for (int depth = 1;
                depth <= maxDepth && frontierStart < frontierEnd && !truncated;
                depth++) {
            int nextStart = size;
            for (int position = frontierStart; position < frontierEnd && !truncated; position++) {
                int vertex = Math.toIntExact(vertices[position]);
                int from = Math.toIntExact(offsets[vertex]);
                int to = Math.toIntExact(offsets[vertex + 1]);
                for (int entry = from; entry < to; entry++) {
                    int neighbor = Math.toIntExact(destinations[entry]);
                    if (discovered[neighbor]) {
                        continue;
                    }
                    if (size == capacity) {
                        truncated = true;
                        break;
                    }
                    discovered[neighbor] = true;
                    vertices[size] = neighbor;
                    depths[size] = depth;
                    size++;
                }
            }
            if (size == nextStart) {
                break;
            }
            Arrays.sort(vertices, nextStart, size);
            frontierStart = nextStart;
            frontierEnd = size;
        }
        return new TraversalResult(
                Arrays.copyOf(vertices, size), Arrays.copyOf(depths, size), truncated);
    }
}

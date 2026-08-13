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
        int[] offsets = csr.rawOffsets();
        int[] destinations = csr.rawDestinations();

        int capacity = Math.toIntExact(Math.min(maxNodes, vertexCount));
        DiscoveredSet discovered = new DiscoveredSet(capacity);
        int[] vertices = new int[capacity];
        int[] depths = new int[capacity];
        int size = 0;
        vertices[size] = (int) start;
        depths[size] = 0;
        size++;
        discovered.add((int) start);

        boolean truncated = false;
        int frontierStart = 0;
        int frontierEnd = 1;
        for (int depth = 1;
                depth <= maxDepth && frontierStart < frontierEnd && !truncated;
                depth++) {
            int nextStart = size;
            for (int position = frontierStart; position < frontierEnd && !truncated; position++) {
                int vertex = vertices[position];
                int from = offsets[vertex];
                int to = offsets[vertex + 1];
                for (int entry = from; entry < to; entry++) {
                    int neighbor = destinations[entry];
                    if (discovered.contains(neighbor)) {
                        continue;
                    }
                    if (size == capacity) {
                        truncated = true;
                        break;
                    }
                    discovered.add(neighbor);
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
        long[] reached = new long[size];
        for (int position = 0; position < size; position++) {
            reached[position] = vertices[position];
        }
        return new TraversalResult(reached, Arrays.copyOf(depths, size), truncated);
    }

    /**
     * An open-addressed set of the vertices already reached, sized by the node budget rather than
     * by the graph.
     *
     * <p>A bitmap over the whole vertex space costs one allocation per request that grows with the
     * graph, which on a graph of tens of millions of vertices dominates a request that is allowed
     * to touch fifty of them. This set is sized by what the request may discover, so the cost of a
     * bounded request stays bounded as the graph grows.
     */
    private static final class DiscoveredSet {
        private static final int EMPTY = -1;

        private final int[] slots;
        private final int mask;

        private DiscoveredSet(int capacity) {
            int size = Integer.highestOneBit(Math.max(4, capacity)) * 4;
            this.slots = new int[size];
            this.mask = size - 1;
            Arrays.fill(slots, EMPTY);
        }

        private int slotOf(int vertex) {
            int slot = (int) ((vertex * 0x9E3779B97F4A7C15L) >>> 40) & mask;
            while (slots[slot] != EMPTY && slots[slot] != vertex) {
                slot = (slot + 1) & mask;
            }
            return slot;
        }

        private boolean contains(int vertex) {
            return slots[slotOf(vertex)] == vertex;
        }

        private void add(int vertex) {
            slots[slotOf(vertex)] = vertex;
        }
    }
}

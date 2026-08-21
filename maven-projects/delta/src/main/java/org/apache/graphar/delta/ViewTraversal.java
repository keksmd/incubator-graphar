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

package org.apache.graphar.delta;

import java.util.Arrays;
import java.util.Objects;

/**
 * Breadth-first expansion over a {@link GraphView} under an explicit depth and node budget.
 *
 * <p>This is the projection's bounded traversal taught to read the delta as well, and it keeps that
 * traversal's two properties. A request cannot pull a whole component into a response, because it
 * stops at the node budget and says so. And a request is deterministic for a given view: each
 * frontier is expanded in ascending vertex order and newly reached vertices are appended in
 * ascending order, whatever order the two halves happened to store the adjacency in.
 *
 * <p>Freshness costs one extra term per visited vertex, the patches the delta holds for it. A
 * traversal over a view therefore stays bounded exactly as long as the delta does.
 */
public final class ViewTraversal {
    private ViewTraversal() {}

    /** Expands at most {@code maxDepth} hops from {@code start}, stopping at {@code maxNodes}. */
    public static ViewTraversalResult neighborhood(
            GraphView view, long start, int maxDepth, int maxNodes) {
        return expand(view, start, maxDepth, maxNodes);
    }

    /**
     * Expands the whole connected component of {@code start}, stopping at {@code maxNodes}. The
     * result is a component only when it is not truncated.
     */
    public static ViewTraversalResult component(GraphView view, long start, int maxNodes) {
        return expand(view, start, Integer.MAX_VALUE, maxNodes);
    }

    private static ViewTraversalResult expand(
            GraphView view, long start, int maxDepth, int maxNodes) {
        Objects.requireNonNull(view, "Graph view cannot be null.");
        if (maxDepth < 0) {
            throw new IllegalArgumentException("Traversal depth must be non-negative: " + maxDepth);
        }
        if (maxNodes < 1) {
            throw new IllegalArgumentException(
                    "Traversal node budget must be positive: " + maxNodes);
        }
        long vertexCount = view.vertexCount();
        if (start < 0 || start >= vertexCount) {
            throw new IllegalArgumentException("Start vertex is outside the view: " + start);
        }

        int capacity = Math.toIntExact(Math.min(maxNodes, vertexCount));
        DiscoveredSet discovered = new DiscoveredSet(capacity);
        Frontier frontier = new Frontier(capacity, discovered);
        frontier.accept(start);

        boolean truncated = false;
        int frontierStart = 0;
        int frontierEnd = frontier.size;
        for (int depth = 1;
                depth <= maxDepth && frontierStart < frontierEnd && !truncated;
                depth++) {
            int nextStart = frontier.size;
            frontier.depth = depth;
            for (int position = frontierStart;
                    position < frontierEnd && !frontier.full;
                    position++) {
                view.forEachNeighbor(frontier.vertices[position], frontier);
            }
            truncated = frontier.full;
            if (frontier.size == nextStart) {
                break;
            }
            Arrays.sort(frontier.vertices, nextStart, frontier.size);
            frontierStart = nextStart;
            frontierEnd = frontier.size;
        }
        long[] reached = new long[frontier.size];
        for (int position = 0; position < frontier.size; position++) {
            reached[position] = frontier.vertices[position];
        }
        return new ViewTraversalResult(
                reached, Arrays.copyOf(frontier.depths, frontier.size), truncated);
    }

    /**
     * The reached vertices, taking each neighbor the view hands over and recording the hop it first
     * appeared at.
     *
     * <p>The view reports the two halves of an adjacency one after the other, so this accumulates
     * rather than returning an array per vertex, which keeps a traversal to one allocation per
     * request whatever the shape of the adjacency it walks.
     */
    private static final class Frontier implements java.util.function.LongConsumer {
        private final int[] vertices;
        private final int[] depths;
        private final DiscoveredSet discovered;
        private int size;
        private int depth;
        private boolean full;

        private Frontier(int capacity, DiscoveredSet discovered) {
            this.vertices = new int[capacity];
            this.depths = new int[capacity];
            this.discovered = discovered;
        }

        @Override
        public void accept(long vertex) {
            int reached = (int) vertex;
            if (full || discovered.contains(reached)) {
                return;
            }
            if (size == vertices.length) {
                full = true;
                return;
            }
            discovered.add(reached);
            vertices[size] = reached;
            depths[size] = depth;
            size++;
        }
    }

    /**
     * An open-addressed set of the vertices already reached, sized by the node budget rather than
     * by the graph, so a bounded request stays bounded as the graph grows.
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

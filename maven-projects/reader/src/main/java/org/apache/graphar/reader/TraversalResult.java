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

/** The vertices reached by one bounded expansion, with the hop at which each was first seen. */
public final class TraversalResult {
    private final long[] vertices;
    private final int[] depths;
    private final boolean truncated;

    TraversalResult(long[] vertices, int[] depths, boolean truncated) {
        this.vertices = vertices;
        this.depths = depths;
        this.truncated = truncated;
    }

    /** Returns the reached vertices, the start vertex first, then each hop in ascending order. */
    public long[] vertices() {
        return vertices.clone();
    }

    /**
     * Returns the hop at which each reached vertex was first seen, aligned with {@link
     * #vertices()}.
     */
    public int[] depths() {
        return depths.clone();
    }

    /** Returns whether the node budget stopped the expansion before it ran out of frontier. */
    public boolean truncated() {
        return truncated;
    }

    /** Returns the number of reached vertices, including the start vertex. */
    public int size() {
        return vertices.length;
    }

    /** Returns the smallest reached vertex, which identifies a component when not truncated. */
    public long componentId() {
        long smallest = vertices[0];
        for (long vertex : vertices) {
            if (vertex < smallest) {
                smallest = vertex;
            }
        }
        return smallest;
    }

    /** Returns the number of vertices first seen at {@code depth}. */
    public int frontierSize(int depth) {
        int count = 0;
        for (int value : depths) {
            if (value == depth) {
                count++;
            }
        }
        return count;
    }

    @Override
    public String toString() {
        return "TraversalResult{size="
                + vertices.length
                + ", truncated="
                + truncated
                + ", frontiers="
                + Arrays.toString(frontierSizes())
                + '}';
    }

    private int[] frontierSizes() {
        int maxDepth = 0;
        for (int value : depths) {
            maxDepth = Math.max(maxDepth, value);
        }
        int[] sizes = new int[maxDepth + 1];
        for (int value : depths) {
            sizes[value]++;
        }
        return sizes;
    }
}

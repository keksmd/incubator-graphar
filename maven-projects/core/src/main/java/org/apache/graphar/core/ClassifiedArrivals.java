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

package org.apache.graphar.core;

import java.util.Arrays;
import java.util.Objects;

/** One arriving batch split into the edges that extend a projection and the edges that patch it. */
public final class ClassifiedArrivals {
    private final AdjacencyOrdering ordering;
    private final AdjacencyFrontier frontier;
    private final EdgeArrivals appendTail;
    private final EdgeArrivals patches;
    private long[] patchedVertexChunks;

    ClassifiedArrivals(
            AdjacencyOrdering ordering,
            AdjacencyFrontier frontier,
            EdgeArrivals appendTail,
            EdgeArrivals patches) {
        this.ordering = Objects.requireNonNull(ordering, "Adjacency ordering cannot be null.");
        this.frontier = Objects.requireNonNull(frontier, "Adjacency frontier cannot be null.");
        this.appendTail = appendTail;
        this.patches = patches;
    }

    /** Returns the ordering the split was decided against. */
    public AdjacencyOrdering ordering() {
        return ordering;
    }

    /** Returns the frontier the split was decided against. */
    public AdjacencyFrontier frontier() {
        return frontier;
    }

    /**
     * Returns the edges that sort past the frontier.
     *
     * <p>They accumulate until they fill a chunk and are then published as new chunks. They arrive
     * in the order the source produced them, not in ordering order, so the stage that writes them
     * still has to sort what it accumulated.
     */
    public EdgeArrivals appendTail() {
        return appendTail;
    }

    /** Returns the edges that change the adjacency of an already materialized vertex. */
    public EdgeArrivals patches() {
        return patches;
    }

    /**
     * Returns the distinct vertex chunks the patches touch, ascending.
     *
     * <p>This is the key a chunk-grouped delta stores patches under, and the chunk set a later
     * compaction would have to rewrite. It is derived on first call, not while classifying, so a
     * caller that only routes the two groups never pays for it.
     */
    public long[] patchedVertexChunks() {
        if (patchedVertexChunks == null) {
            patchedVertexChunks = distinctPatchedVertexChunks();
        }
        return Arrays.copyOf(patchedVertexChunks, patchedVertexChunks.length);
    }

    private long[] distinctPatchedVertexChunks() {
        long[] chunks = new long[patches.count()];
        for (int index = 0; index < chunks.length; index++) {
            chunks[index] =
                    ordering.vertexChunk(
                            ordering.primaryVertex(patches.source(index), patches.target(index)));
        }
        Arrays.sort(chunks);
        int distinct = 0;
        for (int index = 0; index < chunks.length; index++) {
            if (index == 0 || chunks[index] != chunks[distinct - 1]) {
                chunks[distinct++] = chunks[index];
            }
        }
        return Arrays.copyOf(chunks, distinct);
    }
}

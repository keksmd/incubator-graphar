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

package org.apache.graphar.compaction.cost;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Where reads land, chunk by chunk.
 *
 * <p>What the Delta costs a reader depends on which vertices are read, and that is workload, not
 * structure. Rather than assume a distribution, the read penalty is computed against a profile the
 * caller supplies: measured counts when the serving side records them, and {@link
 * #uniformOverVertices} only as the explicit stand-in for a workload not yet measured.
 *
 * <p>Weights are relative; only their proportions matter.
 */
@FunctionalInterface
public interface ReadProfile {
    /** Returns the relative number of reads landing in {@code chunkIndex}. */
    double reads(long chunkIndex);

    /**
     * Returns the profile of a workload that reads every vertex equally often, which weights a
     * chunk by the vertices it covers.
     */
    static ReadProfile uniformOverVertices(BaseChunkLayout layout) {
        Objects.requireNonNull(layout, "Layout cannot be null.");
        return chunkIndex ->
                chunkIndex < layout.chunkCount()
                        ? layout.verticesInChunk(chunkIndex)
                        : layout.vertexChunkSize();
    }

    /** Returns a profile from measured per-chunk read counts; unlisted chunks are never read. */
    static ReadProfile ofCounts(Map<Long, ? extends Number> readsPerChunk) {
        Map<Long, Double> counts = new HashMap<>();
        for (Map.Entry<Long, ? extends Number> entry : readsPerChunk.entrySet()) {
            double reads = entry.getValue().doubleValue();
            if (reads < 0) {
                throw new IllegalArgumentException("Read counts must be non-negative: " + reads);
            }
            counts.put(entry.getKey(), reads);
        }
        return chunkIndex -> counts.getOrDefault(chunkIndex, 0.0d);
    }
}

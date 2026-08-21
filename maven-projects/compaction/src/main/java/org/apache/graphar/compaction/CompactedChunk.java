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

package org.apache.graphar.compaction;

import java.util.Objects;

/** What one chunk-scoped compaction rewrote, and which patch slice it consumed doing so. */
public final class CompactedChunk {
    private final EdgeChunk chunk;
    private final long watermark;
    private final long baseEdgeCount;
    private final long patchEdgeCount;
    private final long edgeCount;
    private final long adjacencyChunkCount;
    private final long bytesRewritten;

    CompactedChunk(
            EdgeChunk chunk,
            long watermark,
            long baseEdgeCount,
            long patchEdgeCount,
            long edgeCount,
            long adjacencyChunkCount,
            long bytesRewritten) {
        this.chunk = Objects.requireNonNull(chunk, "Edge chunk cannot be null.");
        this.watermark = watermark;
        this.baseEdgeCount = baseEdgeCount;
        this.patchEdgeCount = patchEdgeCount;
        this.edgeCount = edgeCount;
        this.adjacencyChunkCount = adjacencyChunkCount;
        this.bytesRewritten = bytesRewritten;
    }

    /** Returns the chunk that was rewritten. */
    public EdgeChunk chunk() {
        return chunk;
    }

    /**
     * Returns the patch-source position folded into the base, which is the exact slice a source
     * drops for this chunk and the exact point after which pending patches are kept.
     */
    public long watermark() {
        return watermark;
    }

    /** Returns how many edge rows the partition held before the rewrite. */
    public long baseEdgeCount() {
        return baseEdgeCount;
    }

    /** Returns how many patch edges were folded in. */
    public long patchEdgeCount() {
        return patchEdgeCount;
    }

    /** Returns how many edge rows the partition holds after the rewrite. */
    public long edgeCount() {
        return edgeCount;
    }

    /** Returns how many GraphAr adjacency chunk files the rewritten partition is stored in. */
    public long adjacencyChunkCount() {
        return adjacencyChunkCount;
    }

    /**
     * Returns the size of everything the rewrite published for this partition: adjacency chunks,
     * aligned edge property chunks, offsets, and the partition edge count. This is the measured
     * base cost of the rewrite, against which the amount of Delta it drained is weighed.
     */
    public long bytesRewritten() {
        return bytesRewritten;
    }

    @Override
    public String toString() {
        return "CompactedChunk{"
                + chunk
                + ", watermark="
                + watermark
                + ", baseEdges="
                + baseEdgeCount
                + ", patchEdges="
                + patchEdgeCount
                + ", edges="
                + edgeCount
                + ", adjacencyChunks="
                + adjacencyChunkCount
                + ", bytesRewritten="
                + bytesRewritten
                + "}";
    }
}

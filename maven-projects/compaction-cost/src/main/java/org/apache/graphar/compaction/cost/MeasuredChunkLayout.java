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

/**
 * A {@link BaseChunkLayout} assembled from sizes read off storage.
 *
 * <p>The caller walks the chunk files of an adjacency layout, adds the size of each chunk together
 * with the entries it holds, and gets a layout whose per-entry byte cost is the compressed cost the
 * dataset actually pays. A chunk never handed to the builder is a chunk of Base that stores
 * nothing, which is a legal state for a vertex range with no outgoing entries, and is reported as
 * zero bytes and zero entries rather than as an error.
 */
public final class MeasuredChunkLayout implements BaseChunkLayout {
    private final long vertexChunkSize;
    private final long vertexCount;
    private final long[] bytes;
    private final long[] edges;

    private MeasuredChunkLayout(
            long vertexChunkSize, long vertexCount, long[] bytes, long[] edges) {
        this.vertexChunkSize = vertexChunkSize;
        this.vertexCount = vertexCount;
        this.bytes = bytes;
        this.edges = edges;
    }

    /**
     * Starts a layout of {@code vertexCount} vertices cut into chunks of {@code vertexChunkSize}.
     */
    public static Builder builder(long vertexChunkSize, long vertexCount) {
        return new Builder(vertexChunkSize, vertexCount);
    }

    @Override
    public long vertexChunkSize() {
        return vertexChunkSize;
    }

    @Override
    public long vertexCount() {
        return vertexCount;
    }

    @Override
    public long baseBytes(long chunkIndex) {
        return bytes[checkedIndex(chunkIndex)];
    }

    @Override
    public long baseEdges(long chunkIndex) {
        return edges[checkedIndex(chunkIndex)];
    }

    private int checkedIndex(long chunkIndex) {
        if (chunkIndex < 0 || chunkIndex >= bytes.length) {
            throw new IllegalArgumentException("Chunk is outside the layout: " + chunkIndex);
        }
        return (int) chunkIndex;
    }

    /** Collects the measured size of each chunk. */
    public static final class Builder {
        private final long vertexChunkSize;
        private final long vertexCount;
        private final long[] bytes;
        private final long[] edges;

        private Builder(long vertexChunkSize, long vertexCount) {
            if (vertexChunkSize <= 0) {
                throw new IllegalArgumentException(
                        "Vertex chunk size must be positive: " + vertexChunkSize);
            }
            if (vertexCount < 0) {
                throw new IllegalArgumentException(
                        "Vertex count must be non-negative: " + vertexCount);
            }
            this.vertexChunkSize = vertexChunkSize;
            this.vertexCount = vertexCount;
            long chunkCount = vertexCount == 0 ? 0 : 1 + (vertexCount - 1) / vertexChunkSize;
            if (chunkCount > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Layout has too many chunks: " + chunkCount);
            }
            this.bytes = new long[(int) chunkCount];
            this.edges = new long[(int) chunkCount];
        }

        /** Records the stored bytes and adjacency entries measured for one chunk. */
        public Builder chunk(long chunkIndex, long chunkBytes, long chunkEdges) {
            if (chunkIndex < 0 || chunkIndex >= bytes.length) {
                throw new IllegalArgumentException("Chunk is outside the layout: " + chunkIndex);
            }
            if (chunkBytes < 0) {
                throw new IllegalArgumentException(
                        "Chunk bytes must be non-negative: " + chunkBytes);
            }
            if (chunkEdges < 0) {
                throw new IllegalArgumentException(
                        "Chunk entries must be non-negative: " + chunkEdges);
            }
            bytes[(int) chunkIndex] = chunkBytes;
            edges[(int) chunkIndex] = chunkEdges;
            return this;
        }

        /** Returns the assembled layout. */
        public MeasuredChunkLayout build() {
            return new MeasuredChunkLayout(
                    vertexChunkSize, vertexCount, bytes.clone(), edges.clone());
        }
    }
}

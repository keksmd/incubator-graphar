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

import org.apache.graphar.core.ChunkMath;

/**
 * The stored size of a Base projection, chunk by chunk.
 *
 * <p>Cost accounting asks one question of Base: what does rewriting this chunk cost. That is a
 * property of the bytes actually on storage, not of the in-memory topology, so this interface is
 * fed by measurement - the size of the files a rewrite of the chunk would have to produce again,
 * adjacency and offsets together - and never by an assumed bytes-per-edge constant.
 *
 * <p>The unit is the vertex chunk, because GraphAr aligns adjacency partitions to vertex chunks and
 * a patch is addressed by the vertex it changes. Everything else in this package derives its chunk
 * index from {@link #vertexChunkSize()} through {@link ChunkMath}, so an implementation reporting a
 * different chunk size than the ledger observed is a mismatch {@link CompactionEstimator} refuses
 * rather than silently averages over.
 */
public interface BaseChunkLayout {
    /** Returns the number of vertices covered by one chunk. */
    long vertexChunkSize();

    /** Returns the number of vertices materialized in Base. */
    long vertexCount();

    /**
     * Returns the stored bytes a rewrite of {@code chunkIndex} would have to write again.
     *
     * @throws IllegalArgumentException when the chunk is not part of this layout
     */
    long baseBytes(long chunkIndex);

    /**
     * Returns the adjacency entries stored in {@code chunkIndex}. Together with {@link #baseBytes}
     * this is what gives a measured per-entry byte cost for the chunk instead of a guessed one.
     *
     * @throws IllegalArgumentException when the chunk is not part of this layout
     */
    long baseEdges(long chunkIndex);

    /** Returns the number of chunks Base is made of. */
    default long chunkCount() {
        return ChunkMath.chunkCount(vertexCount(), vertexChunkSize());
    }

    /** Returns how many vertices {@code chunkIndex} covers, which the last chunk may cut short. */
    default long verticesInChunk(long chunkIndex) {
        if (chunkIndex < 0 || chunkIndex >= chunkCount()) {
            throw new IllegalArgumentException("Chunk is outside the layout: " + chunkIndex);
        }
        return Math.min(vertexChunkSize(), vertexCount() - chunkIndex * vertexChunkSize());
    }
}

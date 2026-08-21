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

import java.util.Objects;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.type.AdjListType;

/**
 * The physical ordering of one GraphAr ordered adjacency layout, expressed as a sort key over edge
 * endpoints.
 *
 * <p>An ordered layout stores edges sorted by one endpoint and, within that endpoint, by the other.
 * This class names those two roles primary and secondary, so that code reasoning about where a row
 * lands does not repeat the source or target branch. The same instance carries the chunk sizes the
 * layout declares, because where a row lands and which chunk stores it are one question asked
 * twice.
 */
public final class AdjacencyOrdering {
    private final AdjListType adjListType;
    private final long vertexChunkSize;
    private final long edgeChunkSize;

    private AdjacencyOrdering(AdjListType adjListType, long vertexChunkSize, long edgeChunkSize) {
        this.adjListType = adjListType;
        this.vertexChunkSize = vertexChunkSize;
        this.edgeChunkSize = edgeChunkSize;
    }

    /** Reads the ordering an edge type declares for one of its ordered adjacency layouts. */
    public static AdjacencyOrdering of(EdgeInfo edgeInfo, AdjListType adjListType) {
        Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        Objects.requireNonNull(adjListType, "Adjacency list type cannot be null.");
        if (!adjListType.isOrdered()) {
            throw new IllegalArgumentException(
                    "An ordered adjacency layout is required: " + adjListType);
        }
        if (!edgeInfo.hasAdjListType(adjListType)) {
            throw new IllegalArgumentException(
                    "Edge info does not declare adjacency layout: " + adjListType);
        }
        long vertexChunkSize =
                adjListType == AdjListType.ordered_by_source
                        ? edgeInfo.getSrcChunkSize()
                        : edgeInfo.getDstChunkSize();
        ChunkMath.validateChunkSize(vertexChunkSize);
        ChunkMath.validateChunkSize(edgeInfo.getChunkSize());
        return new AdjacencyOrdering(adjListType, vertexChunkSize, edgeInfo.getChunkSize());
    }

    /** Returns the layout this ordering describes. */
    public AdjListType adjListType() {
        return adjListType;
    }

    /** Returns the number of primary vertices one offset chunk of this layout covers. */
    public long vertexChunkSize() {
        return vertexChunkSize;
    }

    /** Returns the number of edge rows one adjacency chunk of this layout holds. */
    public long edgeChunkSize() {
        return edgeChunkSize;
    }

    /** Returns the endpoint this ordering sorts by first, which owns the adjacency. */
    public long primaryVertex(long source, long target) {
        return adjListType == AdjListType.ordered_by_source ? source : target;
    }

    /** Returns the endpoint this ordering sorts by within one primary vertex. */
    public long secondaryVertex(long source, long target) {
        return adjListType == AdjListType.ordered_by_source ? target : source;
    }

    /** Returns the vertex chunk whose offset chunk addresses {@code primaryVertex}. */
    public long vertexChunk(long primaryVertex) {
        return ChunkMath.chunkIndex(primaryVertex, vertexChunkSize);
    }
}

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

import java.net.URI;
import java.util.Objects;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.type.AdjListType;

/** Resolves GraphAr ordered adjacency metadata into offset and edge-chunk locations. */
public final class OrderedAdjacencyResolver {
    private final EdgeInfo edgeInfo;
    private final AdjacencyOrdering ordering;
    private final AdjListType adjListType;

    /** Creates a resolver for one ordered adjacency layout of an edge type. */
    public OrderedAdjacencyResolver(EdgeInfo edgeInfo, AdjListType adjListType) {
        this.edgeInfo = Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        this.ordering = AdjacencyOrdering.of(edgeInfo, adjListType);
        this.adjListType = adjListType;
    }

    /** Returns the physical ordering this resolver addresses. */
    public AdjacencyOrdering ordering() {
        return ordering;
    }

    /** Locates the offset pair that the physical reader must fetch for {@code vertexId}. */
    public OffsetLocation locate(long vertexId) {
        long vertexChunkIndex = ordering.vertexChunk(vertexId);
        long offsetIndex = ChunkMath.offsetInChunk(vertexId, ordering.vertexChunkSize());
        URI offsetChunkUri = edgeInfo.getOffsetChunkUri(adjListType, vertexChunkIndex);
        return new OffsetLocation(vertexId, vertexChunkIndex, offsetIndex, offsetChunkUri);
    }

    /** Combines a vertex location and its two ordered-offset values into exact edge chunks. */
    public ResolvedAdjacency resolve(long vertexId, long offsetBegin, long offsetEnd) {
        OffsetLocation offsetLocation = locate(vertexId);
        EdgeRange edgeRange = EdgeRange.fromOffsets(offsetBegin, offsetEnd);
        return resolved(offsetLocation, edgeRange);
    }

    /** Resolves a vertex using a complete, validated offset chunk read by a physical backend. */
    public ResolvedAdjacency resolve(long vertexId, OffsetChunk offsetChunk) {
        OffsetLocation offsetLocation = locate(vertexId);
        Objects.requireNonNull(offsetChunk, "Offset chunk cannot be null.");
        return resolved(offsetLocation, offsetChunk.rangeFor(offsetLocation.offsetIndex()));
    }

    private ResolvedAdjacency resolved(OffsetLocation offsetLocation, EdgeRange edgeRange) {
        return new ResolvedAdjacency(
                edgeInfo,
                adjListType,
                offsetLocation,
                edgeRange,
                edgeRange.edgeChunks(ordering.edgeChunkSize()));
    }
}

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

/** The physical GraphAr chunks selected by one ordered-layout vertex's offset range. */
public final class ResolvedAdjacency {
    private final EdgeInfo edgeInfo;
    private final AdjListType adjListType;
    private final OffsetLocation offsetLocation;
    private final EdgeRange edgeRange;
    private final ChunkRange edgeChunks;

    ResolvedAdjacency(
            EdgeInfo edgeInfo,
            AdjListType adjListType,
            OffsetLocation offsetLocation,
            EdgeRange edgeRange,
            ChunkRange edgeChunks) {
        this.edgeInfo = Objects.requireNonNull(edgeInfo, "Edge info cannot be null.");
        this.adjListType =
                Objects.requireNonNull(adjListType, "Adjacency list type cannot be null.");
        this.offsetLocation =
                Objects.requireNonNull(offsetLocation, "Offset location cannot be null.");
        this.edgeRange = Objects.requireNonNull(edgeRange, "Edge range cannot be null.");
        this.edgeChunks = Objects.requireNonNull(edgeChunks, "Edge chunk range cannot be null.");
    }

    public OffsetLocation offsetLocation() {
        return offsetLocation;
    }

    public EdgeRange edgeRange() {
        return edgeRange;
    }

    public ChunkRange edgeChunks() {
        return edgeChunks;
    }

    public URI edgeCountUri() {
        return edgeInfo.getEdgesNumFileUri(adjListType, offsetLocation.vertexChunkIndex());
    }

    /** Returns an adjacency URI only for an edge chunk selected by this result. */
    public URI adjacencyChunkUri(long edgeChunkIndex) {
        if (!edgeChunks.contains(edgeChunkIndex)) {
            throw new IllegalArgumentException(
                    "Edge chunk "
                            + edgeChunkIndex
                            + " is outside the resolved range ["
                            + edgeChunks.begin()
                            + ", "
                            + edgeChunks.end()
                            + ")");
        }
        return edgeInfo.getAdjacentListChunkUri(
                adjListType, offsetLocation.vertexChunkIndex(), edgeChunkIndex);
    }

    @Override
    public String toString() {
        return "ResolvedAdjacency{adjListType="
                + adjListType
                + ", offsetLocation="
                + offsetLocation
                + ", edgeRange="
                + edgeRange
                + ", edgeChunks="
                + edgeChunks
                + "}";
    }
}

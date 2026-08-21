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

/**
 * The lowest ordering key a projection has not materialized yet.
 *
 * <p>Everything a projection holds sorts strictly below this key, and everything at or above it can
 * be written by appending. A frontier is therefore the whole of what an ingest classification needs
 * to know about the current Base.
 */
public final class AdjacencyFrontier {
    private final long primaryVertex;
    private final long secondaryVertex;

    private AdjacencyFrontier(long primaryVertex, long secondaryVertex) {
        this.primaryVertex = primaryVertex;
        this.secondaryVertex = secondaryVertex;
    }

    /** Returns the frontier of a projection that has materialized nothing. */
    public static AdjacencyFrontier empty() {
        return new AdjacencyFrontier(0, 0);
    }

    /**
     * Returns the frontier of a projection that materialized the primary vertices {@code [0,
     * vertexCount)}.
     *
     * <p>Every edge of an already materialized vertex counts as a patch, including one whose
     * neighbour sorts past the current adjacency of the last vertex. That single vertex is the only
     * pessimism in this reading, and removing it costs a caller the exact highest key in Base,
     * which {@link #at(long, long)} accepts.
     */
    public static AdjacencyFrontier afterVertices(long vertexCount) {
        ChunkMath.validateElementId(vertexCount);
        return new AdjacencyFrontier(vertexCount, 0);
    }

    /** Returns the frontier at an exact ordering key, the lowest key absent from the projection. */
    public static AdjacencyFrontier at(long primaryVertex, long secondaryVertex) {
        ChunkMath.validateElementId(primaryVertex);
        ChunkMath.validateElementId(secondaryVertex);
        return new AdjacencyFrontier(primaryVertex, secondaryVertex);
    }

    /** Returns the first primary vertex whose adjacency the projection does not fully hold. */
    public long primaryVertex() {
        return primaryVertex;
    }

    /** Returns the first secondary vertex not held within {@link #primaryVertex()}. */
    public long secondaryVertex() {
        return secondaryVertex;
    }

    /** Returns whether an ordering key sorts at or above this frontier. */
    public boolean reaches(long primaryVertex, long secondaryVertex) {
        if (primaryVertex != this.primaryVertex) {
            return primaryVertex > this.primaryVertex;
        }
        return secondaryVertex >= this.secondaryVertex;
    }
}

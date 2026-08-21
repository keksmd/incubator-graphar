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
 * Splits arriving edges into the ones a projection can absorb by appending and the ones that patch
 * adjacency it already materialized.
 *
 * <p>The decision is one comparison of the edge's ordering key against the projection's {@link
 * AdjacencyFrontier}, so it costs no lookup, no read of the projection and no allocation per edge,
 * and can sit on the ingest path. It is also purely a property of the ordering: reclassifying the
 * same batch under another {@link AdjacencyOrdering} moves edges between the two groups.
 */
public final class ArrivalClassifier {
    private final AdjacencyOrdering ordering;
    private final AdjacencyFrontier frontier;

    public ArrivalClassifier(AdjacencyOrdering ordering, AdjacencyFrontier frontier) {
        this.ordering = Objects.requireNonNull(ordering, "Adjacency ordering cannot be null.");
        this.frontier = Objects.requireNonNull(frontier, "Adjacency frontier cannot be null.");
    }

    /** Creates a classifier for one ordered adjacency layout of an edge type. */
    public static ArrivalClassifier of(
            EdgeInfo edgeInfo, AdjListType adjListType, AdjacencyFrontier frontier) {
        return new ArrivalClassifier(AdjacencyOrdering.of(edgeInfo, adjListType), frontier);
    }

    /** Returns the ordering this classifier decides against. */
    public AdjacencyOrdering ordering() {
        return ordering;
    }

    /** Returns the frontier this classifier decides against. */
    public AdjacencyFrontier frontier() {
        return frontier;
    }

    /** Returns the ingest route of one arriving edge. */
    public ArrivalClass classify(long source, long target) {
        return isAppendTail(source, target) ? ArrivalClass.APPEND_TAIL : ArrivalClass.PATCH;
    }

    /**
     * Splits the first {@code edgeCount} arriving edges into the two ingest groups.
     *
     * <p>The batch is walked twice, once to size each group and once to fill it, so neither group
     * is over-allocated and no edge is copied more than once.
     */
    public ClassifiedArrivals partition(long[] sources, long[] targets, int edgeCount) {
        Objects.requireNonNull(sources, "Source endpoints cannot be null.");
        Objects.requireNonNull(targets, "Target endpoints cannot be null.");
        if (edgeCount < 0 || edgeCount > sources.length || edgeCount > targets.length) {
            throw new IllegalArgumentException(
                    "Edge count must be in [0, "
                            + Math.min(sources.length, targets.length)
                            + "]: "
                            + edgeCount);
        }
        int appendCount = 0;
        for (int index = 0; index < edgeCount; index++) {
            if (isAppendTail(sources[index], targets[index])) {
                appendCount++;
            }
        }
        long[] appendSources = new long[appendCount];
        long[] appendTargets = new long[appendCount];
        long[] patchSources = new long[edgeCount - appendCount];
        long[] patchTargets = new long[edgeCount - appendCount];
        int appended = 0;
        int patched = 0;
        for (int index = 0; index < edgeCount; index++) {
            long source = sources[index];
            long target = targets[index];
            if (isAppendTail(source, target)) {
                appendSources[appended] = source;
                appendTargets[appended] = target;
                appended++;
            } else {
                patchSources[patched] = source;
                patchTargets[patched] = target;
                patched++;
            }
        }
        return new ClassifiedArrivals(
                ordering,
                frontier,
                new EdgeArrivals(appendSources, appendTargets),
                new EdgeArrivals(patchSources, patchTargets));
    }

    private boolean isAppendTail(long source, long target) {
        ChunkMath.validateElementId(source);
        ChunkMath.validateElementId(target);
        return frontier.reaches(
                ordering.primaryVertex(source, target), ordering.secondaryVertex(source, target));
    }
}

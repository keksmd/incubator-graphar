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

package org.apache.graphar.delta;

import java.util.Objects;
import java.util.function.LongConsumer;
import org.apache.graphar.reader.CsrDirection;
import org.apache.graphar.reader.CsrGraph;
import org.apache.graphar.reader.HeterogeneousCsr;
import org.apache.graphar.reader.VertexIdIndex;

/**
 * One graph made of an immutable projection and the mutable delta that patches it.
 *
 * <p>A base and a delta are only a graph together, and only when they are the pair that was
 * published together: a base republished by compaction alongside a delta that still holds what it
 * absorbed double-counts every absorbed edge. This class is that pair, taken at one instant and
 * never changed afterwards, which is what lets a request read both halves without a lock and
 * without observing a half-applied publication.
 *
 * <p>Adjacency is answered from the base and then from the delta, so a read pays the base's cost
 * plus the patches that arrived for the vertices it actually visits. That second term is the read
 * amplification the delta is budgeted against, and it is bounded by the delta's edge ceiling.
 */
public final class GraphView {
    /** Returned when neither half of the view names an identifier. */
    public static final long ABSENT = -1L;

    private final HeterogeneousCsr base;
    private final DeltaSnapshot delta;

    private GraphView(HeterogeneousCsr base, DeltaSnapshot delta) {
        this.base = base;
        this.delta = delta;
    }

    /** Pairs a projection with a delta snapshot taken against exactly that projection. */
    public static GraphView of(HeterogeneousCsr base, DeltaSnapshot delta) {
        Objects.requireNonNull(base, "Base projection cannot be null.");
        Objects.requireNonNull(delta, "Delta snapshot cannot be null.");
        if (base.vertexCount() != delta.baseVertexCount()) {
            throw new IllegalArgumentException(
                    "Delta snapshot was taken against a base of "
                            + delta.baseVertexCount()
                            + " vertices, not this one of "
                            + base.vertexCount()
                            + ".");
        }
        if (base.direction() != delta.direction()) {
            throw new IllegalArgumentException(
                    "Base projection is "
                            + base.direction()
                            + " where the delta snapshot is "
                            + delta.direction()
                            + ".");
        }
        return new GraphView(base, delta);
    }

    /** Returns the immutable projection this view reads first. */
    public HeterogeneousCsr base() {
        return base;
    }

    /** Returns the delta snapshot this view reads on top of its base. */
    public DeltaSnapshot delta() {
        return delta;
    }

    /** Returns the direction both halves record adjacency in. */
    public CsrDirection direction() {
        return delta.direction();
    }

    /** Returns the number of vertices the base holds and the delta named together. */
    public long vertexCount() {
        return delta.vertexCount();
    }

    /** Returns the number of adjacency entries the base holds. */
    public long baseEdgeCount() {
        return base.edgeCount();
    }

    /** Returns the number of edges the delta adds to the base. */
    public long deltaEdgeCount() {
        return delta.edgeCount();
    }

    /** Reports whether {@code globalIndex} names a vertex that only the delta knows. */
    public boolean isOverlay(long globalIndex) {
        return delta.isOverlay(globalIndex);
    }

    /**
     * Resolves an application identifier to a global vertex number across both halves, or {@link
     * #ABSENT} when neither holds it.
     */
    public long globalIndex(String vertexType, String externalId) {
        long inBase = base.globalIndex(vertexType, externalId);
        if (inBase != VertexIdIndex.ABSENT) {
            return inBase;
        }
        return delta.globalIndex(vertexType, externalId);
    }

    /** Returns the vertex type that owns {@code globalIndex} in either half. */
    public String typeOf(long globalIndex) {
        return delta.isOverlay(globalIndex)
                ? delta.overlayType(globalIndex)
                : base.typeOf(globalIndex);
    }

    /** Returns the number of adjacency entries both halves hold for {@code globalIndex}. */
    public long degree(long globalIndex) {
        requireVertex(globalIndex);
        long inBase = delta.isOverlay(globalIndex) ? 0L : base.csr().degree(globalIndex);
        return inBase + delta.degree(globalIndex);
    }

    /**
     * Returns every global identifier adjacent to {@code globalIndex} in ascending order, taking
     * the base's adjacency and the delta's patches as one sequence.
     *
     * <p>Multiplicity is preserved: an edge the delta repeats appears as many times as it arrived,
     * because neither half deduplicates and the view is not the place to decide that two arrivals
     * of the same pair were one fact.
     */
    public long[] neighbors(long globalIndex) {
        requireVertex(globalIndex);
        long[] fromBase = delta.isOverlay(globalIndex) ? new long[0] : base.neighbors(globalIndex);
        int[] fromDelta = delta.collect(globalIndex);
        if (fromDelta.length == 0) {
            return fromBase;
        }
        long[] merged = new long[fromBase.length + fromDelta.length];
        int baseAt = 0;
        int deltaAt = 0;
        for (int position = 0; position < merged.length; position++) {
            if (deltaAt == fromDelta.length
                    || (baseAt < fromBase.length && fromBase[baseAt] <= fromDelta[deltaAt])) {
                merged[position] = fromBase[baseAt++];
            } else {
                merged[position] = fromDelta[deltaAt++];
            }
        }
        return merged;
    }

    /**
     * Passes every neighbor of {@code globalIndex} to {@code visitor} without materializing the
     * adjacency, in no particular order. Traversals use this; callers that need an order call
     * {@link #neighbors(long)}.
     */
    public void forEachNeighbor(long globalIndex, LongConsumer visitor) {
        requireVertex(globalIndex);
        if (!delta.isOverlay(globalIndex)) {
            CsrGraph csr = base.csr();
            long degree = csr.degree(globalIndex);
            for (long position = 0; position < degree; position++) {
                visitor.accept(csr.neighbor(globalIndex, position));
            }
        }
        delta.forEachNeighbor(globalIndex, neighbor -> visitor.accept(neighbor));
    }

    /** Expands at most {@code maxDepth} hops from {@code start}, stopping at {@code maxNodes}. */
    public ViewTraversalResult neighborhood(long start, int maxDepth, int maxNodes) {
        return ViewTraversal.neighborhood(this, start, maxDepth, maxNodes);
    }

    /**
     * Expands the whole connected component of {@code start}, stopping at {@code maxNodes}. The
     * result is a component only when it is not truncated.
     */
    public ViewTraversalResult component(long start, int maxNodes) {
        return ViewTraversal.component(this, start, maxNodes);
    }

    @Override
    public String toString() {
        return "GraphView{vertices="
                + vertexCount()
                + ", baseEntries="
                + base.edgeCount()
                + ", deltaEdges="
                + delta.edgeCount()
                + ", sequence="
                + delta.sequence()
                + '}';
    }

    private void requireVertex(long globalIndex) {
        if (globalIndex < 0 || globalIndex >= vertexCount()) {
            throw new IllegalArgumentException("Vertex is outside the view: " + globalIndex);
        }
    }
}

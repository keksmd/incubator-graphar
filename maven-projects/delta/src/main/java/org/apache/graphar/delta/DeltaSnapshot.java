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

import java.util.Arrays;
import java.util.Objects;
import java.util.function.IntConsumer;
import org.apache.graphar.reader.CsrDirection;

/**
 * One immutable view of a {@link MutableDelta}, taken at a sequence and readable while the delta
 * keeps growing.
 *
 * <p>A read that spans a base and a delta has to answer from one state of both, or it can report an
 * edge as absent in the base because it was already folded into it and absent from the delta
 * because it was already dropped. The delta numbers every edge it accepts, and a snapshot pins two
 * numbers: the sequence it was taken at, and the sequence the base already contains. What this
 * snapshot shows is exactly the half-open range between them, so an edge is counted once no matter
 * when compaction moved it.
 *
 * <p>Nothing here is copied from the delta. The adjacency of a vertex is a chain of entries in
 * arrival order, newest first: entries newer than {@link #sequence()} are stepped over, and the
 * chain ends where the base takes over. A snapshot therefore costs one small object, and a read
 * costs the patches that arrived for the vertices it visits and nothing else - which is the read
 * amplification the delta is budgeted against.
 */
public final class DeltaSnapshot {
    /** Returned when a vertex type declares no such identifier. */
    public static final long ABSENT = -1L;

    private final CsrDirection direction;
    private final String[] types;
    private final long baseVertexCount;
    private final int overlayCount;
    private final String[] overlayIdentifiers;
    private final int[] overlayTypes;
    private final GrowableIdIndex[] overlayByType;
    private final VertexHeads heads;
    private final int[][] entryEdge;
    private final int[][] entryLink;
    private final int[][] edgeSources;
    private final int[][] edgeTargets;
    private final int chunkShift;
    private final int chunkMask;
    private final long baseSequence;
    private final long sequence;

    DeltaSnapshot(
            CsrDirection direction,
            String[] types,
            long baseVertexCount,
            int overlayCount,
            String[] overlayIdentifiers,
            int[] overlayTypes,
            GrowableIdIndex[] overlayByType,
            VertexHeads heads,
            int[][] entryEdge,
            int[][] entryLink,
            int[][] edgeSources,
            int[][] edgeTargets,
            int chunkShift,
            int chunkMask,
            long baseSequence,
            long sequence) {
        this.direction = direction;
        this.types = types;
        this.baseVertexCount = baseVertexCount;
        this.overlayCount = overlayCount;
        this.overlayIdentifiers = overlayIdentifiers;
        this.overlayTypes = overlayTypes;
        this.overlayByType = overlayByType;
        this.heads = heads;
        this.entryEdge = entryEdge;
        this.entryLink = entryLink;
        this.edgeSources = edgeSources;
        this.edgeTargets = edgeTargets;
        this.chunkShift = chunkShift;
        this.chunkMask = chunkMask;
        this.baseSequence = baseSequence;
        this.sequence = sequence;
    }

    /** Returns the direction the delta records adjacency in, which is the direction of its base. */
    public CsrDirection direction() {
        return direction;
    }

    /** Returns the vertex count of the base this delta extends. */
    public long baseVertexCount() {
        return baseVertexCount;
    }

    /** Returns the number of vertices the delta named that the base does not hold. */
    public int overlayCount() {
        return overlayCount;
    }

    /** Returns the vertex count of the base and the delta together. */
    public long vertexCount() {
        return baseVertexCount + overlayCount;
    }

    /** Returns the sequence this snapshot was taken at; edges below it are visible. */
    public long sequence() {
        return sequence;
    }

    /** Returns the sequence up to which the base already holds the edges. */
    public long baseSequence() {
        return baseSequence;
    }

    /** Returns the number of edges this snapshot adds to its base. */
    public long edgeCount() {
        return sequence - baseSequence;
    }

    /** Returns the number of vertices carrying at least one patch. */
    public int patchedVertexCount() {
        return heads.size();
    }

    /** Returns the source of the edge numbered {@code sequence}. */
    public long edgeSource(long sequence) {
        return read(edgeSources, offsetOf(sequence));
    }

    /** Returns the target of the edge numbered {@code sequence}. */
    public long edgeTarget(long sequence) {
        return read(edgeTargets, offsetOf(sequence));
    }

    /**
     * Resolves an application identifier the delta named to its global vertex number, or {@link
     * #ABSENT} when the delta did not name it. The base resolves its own identifiers; this answers
     * only for vertices that were not in it.
     */
    public long globalIndex(String vertexType, String externalId) {
        Objects.requireNonNull(externalId, "External identifier cannot be null.");
        int ordinal = typeOrdinal(vertexType);
        int overlay = overlayByType[ordinal].lookup(externalId);
        if (overlay == GrowableIdIndex.ABSENT || overlay - baseVertexCount >= overlayCount) {
            return ABSENT;
        }
        return overlay;
    }

    /** Reports whether {@code globalIndex} names a vertex the delta introduced. */
    public boolean isOverlay(long globalIndex) {
        return globalIndex >= baseVertexCount && globalIndex < vertexCount();
    }

    /** Returns the vertex type of a vertex the delta introduced. */
    public String overlayType(long globalIndex) {
        return types[overlayTypes[overlayOrdinal(globalIndex)]];
    }

    /** Returns the application identifier of a vertex the delta introduced. */
    public String overlayIdentifier(long globalIndex) {
        return overlayIdentifiers[overlayOrdinal(globalIndex)];
    }

    /** Returns the number of entries this snapshot adds to the adjacency of {@code vertex}. */
    public int degree(long vertex) {
        Counter counter = new Counter();
        forEachNeighbor(vertex, counter);
        return counter.count;
    }

    /** Returns the entries this snapshot adds to the adjacency of {@code vertex}, ascending. */
    public long[] neighbors(long vertex) {
        int[] collected = collect(vertex);
        long[] neighbors = new long[collected.length];
        for (int position = 0; position < collected.length; position++) {
            neighbors[position] = collected[position];
        }
        return neighbors;
    }

    /**
     * Passes every entry this snapshot adds to the adjacency of {@code vertex} to {@code visitor},
     * newest first. The order is arrival order, not vertex order: a caller that needs the adjacency
     * ordered sorts what it collected.
     */
    public void forEachNeighbor(long vertex, IntConsumer visitor) {
        if (vertex < 0 || vertex >= vertexCount()) {
            throw new IllegalArgumentException("Vertex is outside the view: " + vertex);
        }
        int local = (int) vertex;
        int entry = heads.head(local);
        while (entry != VertexHeads.ABSENT) {
            int chunk = entry >>> chunkShift;
            int slot = entry & chunkMask;
            int edgeOffset = entryEdge[chunk][slot];
            int link = entryLink[chunk][slot];
            long edgeSequence = baseSequence + edgeOffset;
            if (edgeSequence < sequence) {
                int source = read(edgeSources, edgeOffset);
                int target = read(edgeTargets, edgeOffset);
                visitor.accept(source == local ? target : source);
            }
            entry = link;
        }
    }

    int[] collect(long vertex) {
        Collector collector = new Collector();
        forEachNeighbor(vertex, collector);
        int[] neighbors = Arrays.copyOf(collector.values, collector.size);
        Arrays.sort(neighbors);
        return neighbors;
    }

    private static final class Counter implements IntConsumer {
        private int count;

        @Override
        public void accept(int value) {
            count++;
        }
    }

    private static final class Collector implements IntConsumer {
        private int[] values = new int[8];
        private int size;

        @Override
        public void accept(int value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, size * 2);
            }
            values[size++] = value;
        }
    }

    private int overlayOrdinal(long globalIndex) {
        if (!isOverlay(globalIndex)) {
            throw new IllegalArgumentException(
                    "Vertex is not one the delta introduced: " + globalIndex);
        }
        return (int) (globalIndex - baseVertexCount);
    }

    private int typeOrdinal(String vertexType) {
        for (int ordinal = 0; ordinal < types.length; ordinal++) {
            if (types[ordinal].equals(vertexType)) {
                return ordinal;
            }
        }
        throw new IllegalArgumentException("Undeclared vertex type: " + vertexType);
    }

    private int offsetOf(long edgeSequence) {
        if (edgeSequence < baseSequence || edgeSequence >= sequence) {
            throw new IllegalArgumentException(
                    "Edge sequence is outside this snapshot: " + edgeSequence);
        }
        return (int) (edgeSequence - baseSequence);
    }

    private int read(int[][] chunks, int offset) {
        return chunks[offset >>> chunkShift][offset & chunkMask];
    }
}

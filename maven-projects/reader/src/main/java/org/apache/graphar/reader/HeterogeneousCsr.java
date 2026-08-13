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

package org.apache.graphar.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Merges several GraphAr vertex and edge types into one dense identifier space and one CSR.
 *
 * <p>GraphAr numbers vertices densely per vertex type, so a traversal that crosses types cannot use
 * those numbers directly. This class concatenates the per-type ranges in declaration order: type
 * {@code k} occupies {@code [base(k), base(k) + vertexCount(k))} of the global space. Every
 * declared edge triplet is then read once, its endpoints are shifted into that space, and the
 * result is placed into a single CSR. Reversal is well defined here even for a cross-type triplet,
 * because both endpoints live in the same global space.
 *
 * <p>Application identifiers resolve through one {@link VertexIdIndex} per vertex type, so the
 * caller can enter the traversal with the string identifier its API received.
 */
public final class HeterogeneousCsr {
    private final String[] types;
    private final long[] bases;
    private final Map<String, VertexIdIndex> indexByType;
    private final CsrGraph csr;
    private final CsrDirection direction;

    private HeterogeneousCsr(
            String[] types,
            long[] bases,
            Map<String, VertexIdIndex> indexByType,
            CsrGraph csr,
            CsrDirection direction) {
        this.types = types;
        this.bases = bases;
        this.indexByType = indexByType;
        this.csr = csr;
        this.direction = direction;
    }

    /** Starts a declaration of the vertex and edge types to merge. */
    public static Builder builder(GraphReader graph) {
        return new Builder(graph);
    }

    /** Returns the merged topology in global identifier space. */
    public CsrGraph csr() {
        return csr;
    }

    /** Returns the direction the merged adjacency was materialized in. */
    public CsrDirection direction() {
        return direction;
    }

    /**
     * Returns a projection carrying the edges of this one and {@code edgeCount} more, without
     * reading the dataset again.
     *
     * <p>GraphAr chunks are immutable, so a dataset under continuous ingest grows by edges that
     * were not there when this projection was built. Rebuilding derives the unchanged part again;
     * this merges the arriving edges into the adjacency that already holds it. The endpoints are
     * global identifiers of this projection, which is what {@link #globalIndex} returns, and the
     * vertex space is unchanged: a batch that introduces new vertices needs an identifier index
     * that knows them, so it is a rebuild rather than a merge.
     *
     * <p>The returned projection shares the identifier index of this one and leaves this one
     * untouched, so a caller can publish it while requests are still being answered from here.
     */
    public HeterogeneousCsr merge(long[] sources, long[] targets, int edgeCount) {
        Objects.requireNonNull(sources, "Merged sources cannot be null.");
        Objects.requireNonNull(targets, "Merged targets cannot be null.");
        if (edgeCount < 0 || edgeCount > sources.length || edgeCount > targets.length) {
            throw new IllegalArgumentException(
                    "Merged edge count is outside the endpoints supplied: " + edgeCount);
        }
        int[] narrowedSources = new int[edgeCount];
        int[] narrowedTargets = new int[edgeCount];
        for (int edge = 0; edge < edgeCount; edge++) {
            narrowedSources[edge] = Math.toIntExact(sources[edge]);
            narrowedTargets[edge] = Math.toIntExact(targets[edge]);
        }
        CsrGraph merged =
                CsrMaterializer.merge(
                        csr,
                        narrowedSources,
                        narrowedTargets,
                        edgeCount,
                        csr.vertexCount(),
                        direction);
        return new HeterogeneousCsr(types, bases, indexByType, merged, direction);
    }

    /** Returns the number of vertices across every declared vertex type. */
    public long vertexCount() {
        return csr.vertexCount();
    }

    /** Returns the number of CSR entries, which counts both endpoints of an undirected edge. */
    public long edgeCount() {
        return csr.edgeCount();
    }

    /** Returns the declared vertex types in the order that fixes the global identifier space. */
    public List<String> vertexTypes() {
        return List.of(types);
    }

    /** Returns the first global identifier assigned to {@code vertexType}. */
    public long base(String vertexType) {
        return bases[typeOrdinal(vertexType)];
    }

    /**
     * Resolves an application identifier of {@code vertexType} to its global identifier, or {@link
     * VertexIdIndex#ABSENT} when that type declares no such identifier.
     */
    public long globalIndex(String vertexType, String nodeId) {
        VertexIdIndex index = indexByType.get(vertexType);
        if (index == null) {
            throw new IllegalArgumentException("Undeclared vertex type: " + vertexType);
        }
        long local = index.lookup(nodeId);
        if (local == VertexIdIndex.ABSENT) {
            return VertexIdIndex.ABSENT;
        }
        return bases[typeOrdinal(vertexType)] + local;
    }

    /** Returns the vertex type that owns {@code globalIndex}. */
    public String typeOf(long globalIndex) {
        return types[ordinalOf(globalIndex)];
    }

    /** Returns the GraphAr per-type vertex index of {@code globalIndex}. */
    public long localIndex(long globalIndex) {
        return globalIndex - bases[ordinalOf(globalIndex)];
    }

    /** Returns every global identifier adjacent to {@code globalIndex} in the merged topology. */
    public long[] neighbors(long globalIndex) {
        ordinalOf(globalIndex);
        int[] offsets = csr.rawOffsets();
        int[] destinations = csr.rawDestinations();
        int from = offsets[Math.toIntExact(globalIndex)];
        int to = offsets[Math.toIntExact(globalIndex) + 1];
        long[] adjacent = new long[to - from];
        for (int entry = from; entry < to; entry++) {
            adjacent[entry - from] = destinations[entry];
        }
        return adjacent;
    }

    private int typeOrdinal(String vertexType) {
        for (int ordinal = 0; ordinal < types.length; ordinal++) {
            if (types[ordinal].equals(vertexType)) {
                return ordinal;
            }
        }
        throw new IllegalArgumentException("Undeclared vertex type: " + vertexType);
    }

    private int ordinalOf(long globalIndex) {
        if (globalIndex < 0 || globalIndex >= csr.vertexCount()) {
            throw new IllegalArgumentException(
                    "Global vertex identifier is outside the merged space: " + globalIndex);
        }
        int found = Arrays.binarySearch(bases, globalIndex);
        return found >= 0 ? found : -found - 2;
    }

    /** Declares the vertex types, edge triplets, and direction of one merged projection. */
    public static final class Builder {
        private final GraphReader graph;
        private final Map<String, String> idPropertyByType = new LinkedHashMap<>();
        private final Set<EdgeTriplet> triplets = new LinkedHashSet<>();
        private CsrDirection direction = CsrDirection.UNDIRECTED;

        private Builder(GraphReader graph) {
            this.graph = Objects.requireNonNull(graph, "Graph reader cannot be null.");
        }

        /** Adds a vertex type whose identifiers come from its first declared primary property. */
        public Builder addVertexType(String vertexType) {
            return addVertexType(vertexType, null);
        }

        /** Adds a vertex type whose identifiers come from {@code idProperty}. */
        public Builder addVertexType(String vertexType, String idProperty) {
            Objects.requireNonNull(vertexType, "Vertex type cannot be null.");
            if (idPropertyByType.containsKey(vertexType)) {
                throw new IllegalArgumentException("Duplicate vertex type: " + vertexType);
            }
            idPropertyByType.put(vertexType, idProperty);
            return this;
        }

        /** Adds an edge triplet to merge into the projection. */
        public Builder addEdgeType(String srcType, String edgeType, String dstType) {
            triplets.add(new EdgeTriplet(srcType, edgeType, dstType));
            return this;
        }

        /**
         * Sets the direction of the merged adjacency; the default is {@link
         * CsrDirection#UNDIRECTED}.
         */
        public Builder direction(CsrDirection direction) {
            this.direction = Objects.requireNonNull(direction, "CSR direction cannot be null.");
            return this;
        }

        /**
         * Reads every declared type and materializes the merged projection.
         *
         * <p>The topology is read once when its endpoints fit in the heap alongside the projection,
         * and twice when they do not. Buffering the endpoints is the faster of the two and costs
         * eight bytes per edge, which on a large graph is a second copy of the projection; a build
         * that cannot afford that is better served slowly than refused.
         */
        public HeterogeneousCsr build() throws IOException {
            return build(availableHeapBytes());
        }

        HeterogeneousCsr build(long availableBytes) throws IOException {
            if (idPropertyByType.isEmpty()) {
                throw new IllegalArgumentException("At least one vertex type must be declared.");
            }
            String[] types = idPropertyByType.keySet().toArray(new String[0]);
            long[] bases = new long[types.length];
            Map<String, VertexIdIndex> indexByType = new LinkedHashMap<>();
            long nextBase = 0;
            for (int ordinal = 0; ordinal < types.length; ordinal++) {
                String type = types[ordinal];
                bases[ordinal] = nextBase;
                VertexReader vertices = graph.vertex(type);
                String idProperty = idPropertyByType.get(type);
                indexByType.put(
                        type,
                        idProperty == null
                                ? VertexIdIndex.build(vertices)
                                : VertexIdIndex.build(vertices, idProperty));
                nextBase = Math.addExact(nextBase, vertices.vertexCount());
            }
            long totalVertices = nextBase;

            List<OrderedSourceEdgeReader> readers = new ArrayList<>(triplets.size());
            long totalEdges = 0;
            for (EdgeTriplet triplet : triplets) {
                requireDeclared(triplet.srcType, triplet);
                requireDeclared(triplet.dstType, triplet);
                OrderedSourceEdgeReader reader =
                        graph.edge(triplet.srcType, triplet.edgeType, triplet.dstType);
                readers.add(reader);
                totalEdges = Math.addExact(totalEdges, reader.edgeCount());
            }
            ProjectionCapacity.requireAddressable(
                    totalVertices, ProjectionCapacity.entryCount(totalEdges, direction));
            ProjectionCapacity.requireHeadroom(
                    totalVertices, totalEdges, direction, availableBytes);
            if (ProjectionCapacity.peakBuildBytes(totalVertices, totalEdges, direction)
                    > availableBytes) {
                CsrGraph scanned =
                        CsrMaterializer.fromScans(
                                scansOf(readers, types, bases), totalVertices, direction);
                return new HeterogeneousCsr(types, bases, indexByType, scanned, direction);
            }
            int storedEdges = Math.toIntExact(totalEdges);
            int[] sources = new int[storedEdges];
            int[] targets = new int[storedEdges];
            int stored = 0;
            int position = 0;
            for (EdgeTriplet triplet : triplets) {
                long sourceBase = bases[indexOf(types, triplet.srcType)];
                long targetBase = bases[indexOf(types, triplet.dstType)];
                try (EdgeCursor cursor = readers.get(position++).scanEdges()) {
                    while (cursor.next()) {
                        if (stored == storedEdges) {
                            throw new IllegalArgumentException(
                                    "Topology rows exceed GraphAr edge_count control files: "
                                            + triplet);
                        }
                        sources[stored] =
                                Math.toIntExact(Math.addExact(sourceBase, cursor.source()));
                        targets[stored] =
                                Math.toIntExact(Math.addExact(targetBase, cursor.destination()));
                        stored++;
                    }
                }
            }
            if (stored != storedEdges) {
                throw new IllegalArgumentException(
                        "Topology rows do not match GraphAr edge_count control files.");
            }
            CsrGraph csr =
                    CsrMaterializer.fromEndpoints(
                            sources, targets, storedEdges, totalVertices, direction);
            return new HeterogeneousCsr(types, bases, indexByType, csr, direction);
        }

        private List<CsrMaterializer.EndpointScan> scansOf(
                List<OrderedSourceEdgeReader> readers, String[] types, long[] bases) {
            List<CsrMaterializer.EndpointScan> scans = new ArrayList<>(readers.size());
            int position = 0;
            for (EdgeTriplet triplet : triplets) {
                final OrderedSourceEdgeReader reader = readers.get(position++);
                final long sourceBase = bases[indexOf(types, triplet.srcType)];
                final long targetBase = bases[indexOf(types, triplet.dstType)];
                scans.add(
                        sink -> {
                            try (EdgeCursor cursor = reader.scanEdges()) {
                                while (cursor.next()) {
                                    sink.accept(
                                            Math.toIntExact(
                                                    Math.addExact(sourceBase, cursor.source())),
                                            Math.toIntExact(
                                                    Math.addExact(
                                                            targetBase, cursor.destination())));
                                }
                            }
                        });
            }
            return scans;
        }

        private static long availableHeapBytes() {
            Runtime runtime = Runtime.getRuntime();
            return runtime.maxMemory() - runtime.totalMemory() + runtime.freeMemory();
        }

        private void requireDeclared(String vertexType, EdgeTriplet triplet) {
            if (!idPropertyByType.containsKey(vertexType)) {
                throw new IllegalArgumentException(
                        "Edge triplet "
                                + triplet
                                + " uses vertex type "
                                + vertexType
                                + ", which was not declared.");
            }
        }

        private static int indexOf(String[] types, String vertexType) {
            for (int ordinal = 0; ordinal < types.length; ordinal++) {
                if (types[ordinal].equals(vertexType)) {
                    return ordinal;
                }
            }
            throw new IllegalArgumentException("Undeclared vertex type: " + vertexType);
        }
    }

    private static final class EdgeTriplet {
        private final String srcType;
        private final String edgeType;
        private final String dstType;

        private EdgeTriplet(String srcType, String edgeType, String dstType) {
            this.srcType = Objects.requireNonNull(srcType, "Source vertex type cannot be null.");
            this.edgeType = Objects.requireNonNull(edgeType, "Edge type cannot be null.");
            this.dstType =
                    Objects.requireNonNull(dstType, "Destination vertex type cannot be null.");
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof EdgeTriplet)) {
                return false;
            }
            EdgeTriplet that = (EdgeTriplet) other;
            return srcType.equals(that.srcType)
                    && edgeType.equals(that.edgeType)
                    && dstType.equals(that.dstType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(srcType, edgeType, dstType);
        }

        @Override
        public String toString() {
            return srcType + '_' + edgeType + '_' + dstType;
        }
    }
}

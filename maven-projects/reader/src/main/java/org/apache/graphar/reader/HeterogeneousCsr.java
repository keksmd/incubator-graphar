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

    private HeterogeneousCsr(
            String[] types, long[] bases, Map<String, VertexIdIndex> indexByType, CsrGraph csr) {
        this.types = types;
        this.bases = bases;
        this.indexByType = indexByType;
        this.csr = csr;
    }

    /** Starts a declaration of the vertex and edge types to merge. */
    public static Builder builder(GraphReader graph) {
        return new Builder(graph);
    }

    /** Returns the merged topology in global identifier space. */
    public CsrGraph csr() {
        return csr;
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
        long[] offsets = csr.rawOffsets();
        long[] destinations = csr.rawDestinations();
        int from = Math.toIntExact(offsets[Math.toIntExact(globalIndex)]);
        int to = Math.toIntExact(offsets[Math.toIntExact(globalIndex) + 1]);
        return Arrays.copyOfRange(destinations, from, to);
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

        /** Reads every declared type once and materializes the merged projection. */
        public HeterogeneousCsr build() throws IOException {
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
            int storedEdges = Math.toIntExact(totalEdges);
            long[] sources = new long[storedEdges];
            long[] targets = new long[storedEdges];
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
                        sources[stored] = Math.addExact(sourceBase, cursor.source());
                        targets[stored] = Math.addExact(targetBase, cursor.destination());
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
            return new HeterogeneousCsr(types, bases, indexByType, csr);
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

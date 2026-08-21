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
    private final long[] counts;
    private final Map<String, VertexIdIndex> indexByType;
    private final CsrGraph csr;
    private final CsrDirection direction;

    private HeterogeneousCsr(
            String[] types,
            long[] bases,
            long[] counts,
            Map<String, VertexIdIndex> indexByType,
            CsrGraph csr,
            CsrDirection direction) {
        this.types = types;
        this.bases = bases;
        this.counts = counts;
        this.indexByType = indexByType;
        this.csr = csr;
        this.direction = direction;
    }

    /** Starts a declaration of the vertex and edge types to merge. */
    public static Builder builder(GraphReader graph) {
        return new Builder(graph);
    }

    /** Starts a batch of vertices and edges to merge into a projection. */
    public static MergeBatch batch() {
        return new MergeBatch();
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
     * vertex space is unchanged. Use {@link #merge(MergeBatch)} for a batch that also introduces
     * vertices, which is the shape an ingest usually has.
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
        return new HeterogeneousCsr(types, bases, counts, indexByType, merged, direction);
    }

    /**
     * Returns a projection carrying the edges of this one and everything in {@code batch}, without
     * reading the dataset again, including the vertices the batch introduces.
     *
     * <p>An ingest that only ever adds edges between vertices already indexed is the rare case: in
     * an identity graph a new user, device, or address arriving is the mass case, and a projection
     * that could not take one would fall back to a full rebuild exactly when merging is worth most.
     * A batch names its endpoints by application identifier for that reason, because the global
     * identifier of an arriving vertex does not exist until this call assigns it.
     *
     * <p>Every identifier the batch mentions that no index resolves is an arriving vertex. It takes
     * the index that follows the ones its type already stores, in the order the batch first
     * mentions it, which is the numbering an append-only writer gives it in the dataset; a later
     * rebuild over the grown dataset therefore produces this projection again. Arriving vertices
     * push the types declared after theirs along, so the vertices this projection already holds get
     * new global identifiers, and their adjacency is carried over rather than derived again.
     *
     * <p>A global identifier is only meaningful within one projection. It was already unstable
     * across a rebuild, and a merge that grows the vertex space moves it too, so resolve
     * identifiers against the projection answering the request rather than carrying them across a
     * publish.
     *
     * <p>This projection and its identifier indexes are left untouched, so a caller can publish the
     * returned one while requests are still being answered from here.
     *
     * @throws IllegalArgumentException when the batch names a vertex type this projection did not
     *     declare
     */
    public HeterogeneousCsr merge(MergeBatch batch) {
        Objects.requireNonNull(batch, "Merge batch cannot be null.");
        for (String mentionedType : batch.mentionedTypes()) {
            typeOrdinal(mentionedType);
        }
        List<List<String>> arrivingByOrdinal = new ArrayList<>(types.length);
        long[] grownCounts = new long[types.length];
        long[] grownBases = new long[types.length];
        long nextBase = 0;
        boolean grown = false;
        for (int ordinal = 0; ordinal < types.length; ordinal++) {
            VertexIdIndex index = indexByType.get(types[ordinal]);
            List<String> arriving = new ArrayList<>();
            for (String identifier : batch.mentionsOf(types[ordinal])) {
                if (index.lookup(identifier) == VertexIdIndex.ABSENT) {
                    arriving.add(identifier);
                }
            }
            arrivingByOrdinal.add(arriving);
            grown |= !arriving.isEmpty();
            grownBases[ordinal] = nextBase;
            grownCounts[ordinal] = Math.addExact(counts[ordinal], arriving.size());
            nextBase = Math.addExact(nextBase, grownCounts[ordinal]);
        }
        if (!grown) {
            grownBases = bases;
            grownCounts = counts;
        }
        Map<String, VertexIdIndex> grownIndexByType = new LinkedHashMap<>();
        for (int ordinal = 0; ordinal < types.length; ordinal++) {
            grownIndexByType.put(
                    types[ordinal],
                    indexByType
                            .get(types[ordinal])
                            .extendedWith(counts[ordinal], arrivingByOrdinal.get(ordinal)));
        }

        int edgeCount = batch.edgeCount();
        int[] sources = new int[edgeCount];
        int[] targets = new int[edgeCount];
        for (int edge = 0; edge < edgeCount; edge++) {
            sources[edge] =
                    resolve(
                            grownIndexByType,
                            grownBases,
                            batch.sourceType(edge),
                            batch.sourceId(edge));
            targets[edge] =
                    resolve(
                            grownIndexByType,
                            grownBases,
                            batch.targetType(edge),
                            batch.targetId(edge));
        }
        CsrGraph merged =
                CsrMaterializer.merge(
                        csr,
                        sources,
                        targets,
                        edgeCount,
                        nextBase,
                        direction,
                        grown
                                ? new TypeRelocation(bases, counts, grownBases)
                                : CsrMaterializer.VertexRelocation.IDENTITY);
        return new HeterogeneousCsr(
                types, grownBases, grownCounts, grownIndexByType, merged, direction);
    }

    private int resolve(
            Map<String, VertexIdIndex> indexes, long[] bases, String vertexType, String vertexId) {
        long local = indexes.get(vertexType).lookup(vertexId);
        if (local == VertexIdIndex.ABSENT) {
            throw new IllegalStateException(
                    "Merged batch names "
                            + vertexType
                            + ' '
                            + vertexId
                            + ", which stayed unknown.");
        }
        return Math.toIntExact(bases[typeOrdinal(vertexType)] + local);
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

    /**
     * A batch of vertices and edges to merge into a projection, named by application identifier.
     *
     * <p>The batch does not know which of its identifiers the projection already holds, because the
     * same batch can be merged into projections that were built at different times. It records the
     * order it first mentions each identifier, and {@link HeterogeneousCsr#merge(MergeBatch)}
     * decides which of them arrive.
     *
     * <p>Instances are not thread-safe; fill one from a single ingest thread.
     */
    public static final class MergeBatch {
        private final Map<String, Set<String>> mentionsByType = new LinkedHashMap<>();
        private final List<String> sourceTypes = new ArrayList<>();
        private final List<String> sourceIds = new ArrayList<>();
        private final List<String> targetTypes = new ArrayList<>();
        private final List<String> targetIds = new ArrayList<>();

        private MergeBatch() {}

        /**
         * Adds a vertex, which enters the projection whether or not an edge of this batch names it.
         */
        public MergeBatch addVertex(String vertexType, String vertexId) {
            mention(vertexType, vertexId);
            return this;
        }

        /** Adds an edge, and with it both of its endpoints. */
        public MergeBatch addEdge(String srcType, String srcId, String dstType, String dstId) {
            mention(srcType, srcId);
            mention(dstType, dstId);
            sourceTypes.add(srcType);
            sourceIds.add(srcId);
            targetTypes.add(dstType);
            targetIds.add(dstId);
            return this;
        }

        /** Returns the number of edges the batch carries. */
        public int edgeCount() {
            return sourceIds.size();
        }

        /** Returns the number of distinct identifiers the batch mentions. */
        public int mentionCount() {
            int mentions = 0;
            for (Set<String> byType : mentionsByType.values()) {
                mentions += byType.size();
            }
            return mentions;
        }

        private void mention(String vertexType, String vertexId) {
            Objects.requireNonNull(vertexType, "Batch vertex type cannot be null.");
            Objects.requireNonNull(vertexId, "Batch vertex identifier cannot be null.");
            mentionsByType.computeIfAbsent(vertexType, type -> new LinkedHashSet<>()).add(vertexId);
        }

        private Set<String> mentionedTypes() {
            return mentionsByType.keySet();
        }

        private Set<String> mentionsOf(String vertexType) {
            return mentionsByType.getOrDefault(vertexType, Set.of());
        }

        private String sourceType(int edge) {
            return sourceTypes.get(edge);
        }

        private String sourceId(int edge) {
            return sourceIds.get(edge);
        }

        private String targetType(int edge) {
            return targetTypes.get(edge);
        }

        private String targetId(int edge) {
            return targetIds.get(edge);
        }
    }

    /**
     * Shifts the vertices of a projection into the space the same types occupy once vertices have
     * arrived for some of them. Each type keeps its range contiguous and in declaration order, so
     * the mapping is order-preserving, which is what a merge needs to keep an adjacency sorted.
     */
    private static final class TypeRelocation implements CsrMaterializer.VertexRelocation {
        private final long[] bases;
        private final long[] counts;
        private final long[] grownBases;

        private TypeRelocation(long[] bases, long[] counts, long[] grownBases) {
            this.bases = bases;
            this.counts = counts;
            this.grownBases = grownBases;
        }

        @Override
        public int relocate(int baseVertex) {
            int ordinal = ordinalIn(bases, baseVertex);
            return Math.toIntExact(grownBases[ordinal] + (baseVertex - bases[ordinal]));
        }

        @Override
        public int origin(int mergedVertex) {
            int ordinal = ordinalIn(grownBases, mergedVertex);
            long local = mergedVertex - grownBases[ordinal];
            return local < counts[ordinal]
                    ? Math.toIntExact(bases[ordinal] + local)
                    : CsrMaterializer.VertexRelocation.ABSENT;
        }

        /**
         * Returns the type that owns {@code vertex}, skipping past types that declare no vertex and
         * therefore share the start of the one that follows them.
         */
        private static int ordinalIn(long[] starts, long vertex) {
            int found = Arrays.binarySearch(starts, vertex);
            int ordinal = found >= 0 ? found : -found - 2;
            while (ordinal + 1 < starts.length && starts[ordinal + 1] == starts[ordinal]) {
                ordinal++;
            }
            return ordinal;
        }
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
            long[] counts = new long[types.length];
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
                counts[ordinal] = vertices.vertexCount();
                nextBase = Math.addExact(nextBase, counts[ordinal]);
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
                return new HeterogeneousCsr(types, bases, counts, indexByType, scanned, direction);
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
            return new HeterogeneousCsr(types, bases, counts, indexByType, csr, direction);
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

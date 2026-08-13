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
import java.util.Arrays;
import java.util.List;
import org.apache.graphar.info.EdgeInfo;

/** Builds a bounded heap CSR representation from an ordered source topology scan. */
final class CsrMaterializer {
    private CsrMaterializer() {}

    static CsrGraph materialize(
            OrderedSourceEdgeReader reader,
            EdgeInfo edgeInfo,
            long maxVertices,
            long maxEdges,
            CsrDirection direction)
            throws IOException {
        validateBound(maxVertices, "vertex");
        validateBound(maxEdges, "edge");
        if (direction != CsrDirection.OUTGOING
                && !edgeInfo.getSrcType().equals(edgeInfo.getDstType())) {
            throw new IllegalArgumentException(
                    "CSR direction "
                            + direction
                            + " reverses edges, which is defined only within one vertex type, but "
                            + edgeInfo.getConcat()
                            + " joins two.");
        }
        long vertexCount = reader.vertexCount();
        long edgeCount = reader.edgeCount();
        if (vertexCount > maxVertices || edgeCount > maxEdges) {
            throw new IllegalArgumentException(
                    "Graph exceeds configured CSR bounds: vertices="
                            + vertexCount
                            + ", edges="
                            + edgeCount);
        }
        if (direction == CsrDirection.OUTGOING) {
            return materializeOutgoing(reader, vertexCount, edgeCount);
        }
        return materializeTransposed(reader, vertexCount, edgeCount, direction);
    }

    private static CsrGraph materializeOutgoing(
            OrderedSourceEdgeReader reader, long vertexCount, long edgeCount) throws IOException {
        ProjectionCapacity.requireAddressable(vertexCount, edgeCount);
        int vertexArrayLength = Math.toIntExact(Math.addExact(vertexCount, 1));
        int edgeArrayLength = Math.toIntExact(edgeCount);
        int[] offsets = new int[vertexArrayLength];
        int[] destinations = new int[edgeArrayLength];
        int nextOffset = 1;
        int nextDestination = 0;
        long previousSource = -1;
        try (EdgeCursor cursor = reader.scanEdges()) {
            while (cursor.next()) {
                long source = cursor.source();
                if (source >= vertexCount || source < previousSource) {
                    throw new IllegalArgumentException(
                            "ordered_by_source scan is not sorted and in vertex bounds: " + source);
                }
                while (nextOffset <= source) {
                    offsets[nextOffset++] = nextDestination;
                }
                if (nextDestination == destinations.length) {
                    throw new IllegalArgumentException(
                            "Topology rows exceed GraphAr edge_count control files.");
                }
                destinations[nextDestination++] = Math.toIntExact(cursor.destination());
                previousSource = source;
            }
        }
        if (nextDestination != destinations.length) {
            throw new IllegalArgumentException(
                    "Topology rows do not match GraphAr edge_count control files.");
        }
        while (nextOffset < offsets.length) {
            offsets[nextOffset++] = nextDestination;
        }
        return new CsrGraph(offsets, destinations);
    }

    /**
     * Reads the topology once into endpoint buffers, then places every entry by counting sort. The
     * buffers cost eight bytes per stored edge and replace a second full scan of the dataset, which
     * is the dominant cost of building a transposed adjacency from Parquet chunks.
     */
    private static CsrGraph materializeTransposed(
            OrderedSourceEdgeReader reader,
            long vertexCount,
            long edgeCount,
            CsrDirection direction)
            throws IOException {
        int storedEdges = Math.toIntExact(edgeCount);
        int[] sources = new int[storedEdges];
        int[] targets = new int[storedEdges];
        int stored = 0;
        long previousSource = -1;
        try (EdgeCursor cursor = reader.scanEdges()) {
            while (cursor.next()) {
                long source = cursor.source();
                long destination = cursor.destination();
                if (source >= vertexCount || source < previousSource) {
                    throw new IllegalArgumentException(
                            "ordered_by_source scan is not sorted and in vertex bounds: " + source);
                }
                if (destination < 0 || destination >= vertexCount) {
                    throw new IllegalArgumentException(
                            "Topology destination is outside the vertex type: " + destination);
                }
                if (stored == storedEdges) {
                    throw new IllegalArgumentException(
                            "Topology rows exceed GraphAr edge_count control files.");
                }
                sources[stored] = (int) source;
                targets[stored] = (int) destination;
                stored++;
                previousSource = source;
            }
        }
        if (stored != storedEdges) {
            throw new IllegalArgumentException(
                    "Topology rows do not match GraphAr edge_count control files.");
        }
        return fromEndpoints(sources, targets, storedEdges, vertexCount, direction);
    }

    /**
     * Places buffered endpoint pairs into a CSR by counting sort. Callers that merge several
     * topologies into one identifier space reach this directly, because their edges no longer
     * arrive sorted by source and cannot use the streaming path.
     *
     * <p>GraphAr stores topology {@code ordered_by_source}, so the endpoints a real dataset
     * produces arrive sorted even here. That is not a convenience, it is the difference between two
     * placement strategies: on sorted input the outgoing entries of a vertex form one contiguous
     * run that can be written sequentially, the incoming entries of a vertex arrive in ascending
     * order, and the adjacency of a vertex is therefore two ascending runs that merge in linear
     * time instead of one unordered range that has to be sorted. The strategy is chosen from the
     * input rather than declared by the caller, so a caller that cannot promise the order still
     * gets a correct graph.
     */
    static CsrGraph fromEndpoints(
            int[] sources, int[] targets, int edgeCount, long vertexCount, CsrDirection direction)
            throws IOException {
        long entries = ProjectionCapacity.entryCount(edgeCount, direction);
        ProjectionCapacity.requireAddressable(vertexCount, entries);
        int vertexArrayLength = Math.toIntExact(Math.addExact(vertexCount, 1));
        int entryCount = Math.toIntExact(entries);
        int[] offsets = new int[vertexArrayLength];
        boolean sourcesAscend = true;
        boolean targetsAscendWithinRun = true;
        int previousSource = -1;
        int previousTarget = -1;
        for (int edge = 0; edge < edgeCount; edge++) {
            int source = sources[edge];
            int target = targets[edge];
            if (source < previousSource) {
                sourcesAscend = false;
            } else if (source == previousSource && target < previousTarget) {
                targetsAscendWithinRun = false;
            }
            previousSource = source;
            previousTarget = target;
            if (direction != CsrDirection.OUTGOING) {
                offsets[target + 1]++;
            }
            if (direction != CsrDirection.INCOMING) {
                offsets[source + 1]++;
            }
        }
        for (int vertex = 1; vertex < vertexArrayLength; vertex++) {
            offsets[vertex] += offsets[vertex - 1];
        }
        int[] destinations = new int[entryCount];
        int[] cursorByVertex = offsets.clone();
        if (sourcesAscend) {
            placeSorted(sources, targets, edgeCount, direction, destinations, cursorByVertex);
            mergeRuns(
                    offsets,
                    cursorByVertex,
                    destinations,
                    direction,
                    direction == CsrDirection.INCOMING || targetsAscendWithinRun);
            return new CsrGraph(offsets, destinations);
        }
        for (int edge = 0; edge < edgeCount; edge++) {
            if (direction != CsrDirection.OUTGOING) {
                destinations[cursorByVertex[targets[edge]]++] = sources[edge];
            }
            if (direction != CsrDirection.INCOMING) {
                destinations[cursorByVertex[sources[edge]]++] = targets[edge];
            }
        }
        for (int vertex = 0; vertex < vertexArrayLength - 1; vertex++) {
            Arrays.sort(destinations, offsets[vertex], offsets[vertex + 1]);
        }
        return new CsrGraph(offsets, destinations);
    }

    /**
     * Places entries knowing the endpoints are sorted by source. The incoming entries go first, so
     * that the cursor is left standing on the boundary between the two runs of every vertex, which
     * the merge then reads instead of recomputing it into another vertex-sized array. The outgoing
     * run of a vertex is written from the cursor without advancing it, which is sound only because
     * sorted input delivers that run contiguously.
     */
    private static void placeSorted(
            int[] sources,
            int[] targets,
            int edgeCount,
            CsrDirection direction,
            int[] destinations,
            int[] cursorByVertex) {
        if (direction != CsrDirection.OUTGOING) {
            for (int edge = 0; edge < edgeCount; edge++) {
                destinations[cursorByVertex[targets[edge]]++] = sources[edge];
            }
        }
        if (direction != CsrDirection.INCOMING) {
            int edge = 0;
            while (edge < edgeCount) {
                int source = sources[edge];
                int position = cursorByVertex[source];
                while (edge < edgeCount && sources[edge] == source) {
                    destinations[position++] = targets[edge++];
                }
            }
        }
    }

    /**
     * Orders every adjacency that {@link #placeSorted} left as two runs. When the outgoing run is
     * ascending the two runs merge in linear time; otherwise the range is sorted, which is still
     * cheaper than the unsorted path because the placement was sequential.
     */
    private static void mergeRuns(
            int[] offsets,
            int[] boundaries,
            int[] destinations,
            CsrDirection direction,
            boolean outgoingAscends) {
        if (direction != CsrDirection.UNDIRECTED) {
            if (!outgoingAscends) {
                for (int vertex = 0; vertex < offsets.length - 1; vertex++) {
                    Arrays.sort(destinations, offsets[vertex], offsets[vertex + 1]);
                }
            }
            return;
        }
        int[] buffer = new int[0];
        for (int vertex = 0; vertex < offsets.length - 1; vertex++) {
            int start = offsets[vertex];
            int split = boundaries[vertex];
            int end = offsets[vertex + 1];
            if (start == split || split == end) {
                if (!outgoingAscends) {
                    Arrays.sort(destinations, start, end);
                }
                continue;
            }
            if (!outgoingAscends) {
                Arrays.sort(destinations, split, end);
            }
            int leading = split - start;
            if (buffer.length < leading) {
                buffer = new int[Math.max(leading, buffer.length * 2)];
            }
            System.arraycopy(destinations, start, buffer, 0, leading);
            int left = 0;
            int right = split;
            int write = start;
            while (left < leading && right < end) {
                destinations[write++] =
                        buffer[left] <= destinations[right]
                                ? buffer[left++]
                                : destinations[right++];
            }
            while (left < leading) {
                destinations[write++] = buffer[left++];
            }
        }
    }

    /**
     * Places entries by reading the topology twice instead of buffering its endpoints.
     *
     * <p>The buffered path holds eight bytes per edge for the whole build, which on a graph of
     * hundreds of millions of edges is a second copy of the projection. This path pays a second
     * scan of the dataset instead: the first counts degrees, the second places entries. It is the
     * slower of the two and the only one that can build a graph whose endpoints do not fit
     * alongside it.
     */
    static CsrGraph fromScans(List<EndpointScan> scans, long vertexCount, CsrDirection direction)
            throws IOException {
        int vertexArrayLength = Math.toIntExact(Math.addExact(vertexCount, 1));
        int[] offsets = new int[vertexArrayLength];
        for (EndpointScan scan : scans) {
            scan.scan(
                    (source, target) -> {
                        if (direction != CsrDirection.OUTGOING) {
                            offsets[target + 1]++;
                        }
                        if (direction != CsrDirection.INCOMING) {
                            offsets[source + 1]++;
                        }
                    });
        }
        long entries = 0;
        for (int vertex = 1; vertex < vertexArrayLength; vertex++) {
            entries += offsets[vertex];
        }
        ProjectionCapacity.requireAddressable(vertexCount, entries);
        for (int vertex = 1; vertex < vertexArrayLength; vertex++) {
            offsets[vertex] += offsets[vertex - 1];
        }
        int[] destinations = new int[Math.toIntExact(entries)];
        int[] cursorByVertex = offsets.clone();
        for (EndpointScan scan : scans) {
            scan.scan(
                    (source, target) -> {
                        if (direction != CsrDirection.OUTGOING) {
                            destinations[cursorByVertex[target]++] = source;
                        }
                        if (direction != CsrDirection.INCOMING) {
                            destinations[cursorByVertex[source]++] = target;
                        }
                    });
        }
        for (int vertex = 0; vertex < vertexArrayLength - 1; vertex++) {
            Arrays.sort(destinations, offsets[vertex], offsets[vertex + 1]);
        }
        return new CsrGraph(offsets, destinations);
    }

    /** One readable pass over a topology, in the global identifier space of the projection. */
    @FunctionalInterface
    interface EndpointScan {
        void scan(EndpointSink sink) throws IOException;
    }

    /** Receives the endpoints of one edge during an {@link EndpointScan}. */
    @FunctionalInterface
    interface EndpointSink {
        void accept(int source, int target);
    }

    private static void validateBound(long value, String kind) {
        if (value < 0 || value > Integer.MAX_VALUE - 1L) {
            throw new IllegalArgumentException(
                    "CSR " + kind + " bound must be between zero and " + (Integer.MAX_VALUE - 1L));
        }
    }
}

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
     */
    static CsrGraph fromEndpoints(
            int[] sources, int[] targets, int edgeCount, long vertexCount, CsrDirection direction)
            throws IOException {
        long entries = ProjectionCapacity.entryCount(edgeCount, direction);
        ProjectionCapacity.requireAddressable(vertexCount, entries);
        int vertexArrayLength = Math.toIntExact(Math.addExact(vertexCount, 1));
        int entryCount = Math.toIntExact(entries);
        int[] offsets = new int[vertexArrayLength];
        for (int edge = 0; edge < edgeCount; edge++) {
            if (direction != CsrDirection.OUTGOING) {
                offsets[targets[edge] + 1]++;
            }
            if (direction != CsrDirection.INCOMING) {
                offsets[sources[edge] + 1]++;
            }
        }
        for (int vertex = 1; vertex < vertexArrayLength; vertex++) {
            offsets[vertex] += offsets[vertex - 1];
        }
        int[] destinations = new int[entryCount];
        int[] cursorByVertex = offsets.clone();
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

    private static void validateBound(long value, String kind) {
        if (value < 0 || value > Integer.MAX_VALUE - 1L) {
            throw new IllegalArgumentException(
                    "CSR " + kind + " bound must be between zero and " + (Integer.MAX_VALUE - 1L));
        }
    }
}

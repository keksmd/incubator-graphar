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

/** Builds a bounded heap CSR representation from an ordered source topology scan. */
final class CsrMaterializer {
    private CsrMaterializer() {}

    static CsrGraph materialize(OrderedSourceEdgeReader reader, long maxVertices, long maxEdges)
            throws IOException {
        validateBound(maxVertices, "vertex");
        validateBound(maxEdges, "edge");
        long vertexCount = reader.vertexCount();
        long edgeCount = reader.edgeCount();
        if (vertexCount > maxVertices || edgeCount > maxEdges) {
            throw new IllegalArgumentException(
                    "Graph exceeds configured CSR bounds: vertices="
                            + vertexCount
                            + ", edges="
                            + edgeCount);
        }
        int vertexArrayLength = Math.toIntExact(Math.addExact(vertexCount, 1));
        int edgeArrayLength = Math.toIntExact(edgeCount);
        long[] offsets = new long[vertexArrayLength];
        long[] destinations = new long[edgeArrayLength];
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
                destinations[nextDestination++] = cursor.destination();
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

    private static void validateBound(long value, String kind) {
        if (value < 0 || value > Integer.MAX_VALUE - 1L) {
            throw new IllegalArgumentException(
                    "CSR " + kind + " bound must be between zero and " + (Integer.MAX_VALUE - 1L));
        }
    }
}

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

public class CsrDirectionFixtureTest {
    private static final long MAX_VERTICES = 10_000L;
    private static final long MAX_EDGES = 100_000L;
    private static final long BENCHMARK_VERTEX = 297L;

    @Test
    public void transposesEveryStoredEdgeIntoTheIncomingAdjacency() throws Exception {
        OrderedSourceEdgeReader edges = openEdges();
        Map<Long, List<Long>> expected = new HashMap<>();
        try (EdgeCursor cursor = edges.scanEdges()) {
            while (cursor.next()) {
                expected.computeIfAbsent(cursor.destination(), key -> new ArrayList<>())
                        .add(cursor.source());
            }
        }
        expected.values().forEach(java.util.Collections::sort);

        CsrGraph incoming = edges.materializeCsr(MAX_VERTICES, MAX_EDGES, CsrDirection.INCOMING);

        assertEquals(903L, incoming.vertexCount());
        assertEquals(6626L, incoming.edgeCount());
        for (long vertex = 0; vertex < 903L; vertex++) {
            assertEquals(
                    "incoming adjacency of " + vertex,
                    expected.getOrDefault(vertex, List.of()),
                    adjacency(incoming, vertex));
        }
    }

    @Test
    public void mergesBothDirectionsIntoTheUndirectedAdjacency() throws Exception {
        OrderedSourceEdgeReader edges = openEdges();
        Map<Long, List<Long>> expected = new HashMap<>();
        try (EdgeCursor cursor = edges.scanEdges()) {
            while (cursor.next()) {
                expected.computeIfAbsent(cursor.source(), key -> new ArrayList<>())
                        .add(cursor.destination());
                expected.computeIfAbsent(cursor.destination(), key -> new ArrayList<>())
                        .add(cursor.source());
            }
        }
        expected.values().forEach(java.util.Collections::sort);

        CsrGraph undirected =
                edges.materializeCsr(MAX_VERTICES, MAX_EDGES, CsrDirection.UNDIRECTED);

        assertEquals(903L, undirected.vertexCount());
        assertEquals(13252L, undirected.edgeCount());
        for (long vertex = 0; vertex < 903L; vertex++) {
            assertEquals(
                    "undirected adjacency of " + vertex,
                    expected.getOrDefault(vertex, List.of()),
                    adjacency(undirected, vertex));
        }
    }

    @Test
    public void undirectedDegreeExceedsOutgoingDegreeAtTheBenchmarkVertex() throws Exception {
        OrderedSourceEdgeReader edges = openEdges();
        CsrGraph outgoing = edges.materializeCsr(MAX_VERTICES, MAX_EDGES, CsrDirection.OUTGOING);
        CsrGraph incoming = edges.materializeCsr(MAX_VERTICES, MAX_EDGES, CsrDirection.INCOMING);
        CsrGraph undirected =
                edges.materializeCsr(MAX_VERTICES, MAX_EDGES, CsrDirection.UNDIRECTED);

        List<Long> out = adjacency(outgoing, BENCHMARK_VERTEX);
        List<Long> in = adjacency(incoming, BENCHMARK_VERTEX);
        List<Long> both = adjacency(undirected, BENCHMARK_VERTEX);

        assertEquals(53, out.size());
        assertEquals(out.size() + in.size(), both.size());
        assertTrue("undirected must not lose reachability", both.containsAll(out));
        assertTrue("undirected must add the reverse direction", both.containsAll(in));
        assertTrue("reverse direction must be non-empty at this vertex", !in.isEmpty());
    }

    @Test
    public void outgoingMaterializationIsUnchangedByTheNewOverload() throws Exception {
        OrderedSourceEdgeReader edges = openEdges();
        CsrGraph legacy = edges.materializeCsr(MAX_VERTICES, MAX_EDGES);
        CsrGraph explicit = edges.materializeCsr(MAX_VERTICES, MAX_EDGES, CsrDirection.OUTGOING);

        assertEquals(legacy.vertexCount(), explicit.vertexCount());
        assertEquals(legacy.edgeCount(), explicit.edgeCount());
        assertTrue(java.util.Arrays.equals(legacy.offsets(), explicit.offsets()));
        assertTrue(java.util.Arrays.equals(legacy.destinations(), explicit.destinations()));
    }

    @Test
    public void everyUndirectedEntryHasItsMirror() throws Exception {
        CsrGraph undirected =
                openEdges().materializeCsr(MAX_VERTICES, MAX_EDGES, CsrDirection.UNDIRECTED);
        long[] offsets = undirected.offsets();
        long[] destinations = undirected.destinations();
        Map<String, Integer> counts = new HashMap<>();
        for (long vertex = 0; vertex < undirected.vertexCount(); vertex++) {
            for (long slot = offsets[(int) vertex]; slot < offsets[(int) vertex + 1]; slot++) {
                long neighbor = destinations[(int) slot];
                counts.merge(vertex + ">" + neighbor, 1, Integer::sum);
            }
        }
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            String[] pair = entry.getKey().split(">");
            String mirror = pair[1] + ">" + pair[0];
            assertEquals("mirror of " + entry.getKey(), entry.getValue(), counts.get(mirror));
        }
    }

    @Test
    public void refusesToTransposeAcrossTwoVertexTypes() throws Exception {
        GraphReader graph = openGraph();
        OrderedSourceEdgeReader edges = graph.edge("person", "knows", "person");
        org.apache.graphar.info.EdgeInfo declared =
                graph.graphInfo().getEdgeInfo("person", "knows", "person");
        org.apache.graphar.info.EdgeInfo crossType =
                org.apache.graphar.info.EdgeInfo.builder()
                        .edgeTriplet("user", "uses", "device")
                        .chunkSize(declared.getChunkSize())
                        .srcChunkSize(declared.getSrcChunkSize())
                        .dstChunkSize(declared.getDstChunkSize())
                        .directed(true)
                        .prefix("edge/user_uses_device/")
                        .version("gar/v1")
                        .addPropertyGroups(declared.getPropertyGroups())
                        .adjacentLists(declared.getAdjacentLists())
                        .build();
        for (CsrDirection direction : List.of(CsrDirection.INCOMING, CsrDirection.UNDIRECTED)) {
            try {
                CsrMaterializer.materialize(edges, crossType, MAX_VERTICES, MAX_EDGES, direction);
                throw new AssertionError("Expected " + direction + " to be refused.");
            } catch (IllegalArgumentException expected) {
                assertTrue(expected.getMessage().contains("one vertex type"));
            }
        }
        assertEquals(
                6626L,
                CsrMaterializer.materialize(
                                edges, crossType, MAX_VERTICES, MAX_EDGES, CsrDirection.OUTGOING)
                        .edgeCount());
    }

    private static List<Long> adjacency(CsrGraph graph, long vertex) {
        long[] offsets = graph.offsets();
        long[] destinations = graph.destinations();
        List<Long> neighbors = new ArrayList<>();
        for (long slot = offsets[(int) vertex]; slot < offsets[(int) vertex + 1]; slot++) {
            neighbors.add(destinations[(int) slot]);
        }
        return neighbors;
    }

    private static OrderedSourceEdgeReader openEdges() throws IOException {
        return openGraph().edge("person", "knows", "person");
    }

    private static GraphReader openGraph() throws IOException {
        return GraphReader.open(
                fixturePath().resolve("ldbc_sample.graph.yml").toUri(),
                new LocalFileSystemStringGraphInfoLoader(),
                new LocalStorage(),
                parquetReader());
    }

    private static PhysicalReader parquetReader() {
        return new ParquetPhysicalReader(new LocalStorage());
    }

    private static Path fixturePath() {
        return Path.of("..", "..", "testing", "ldbc_sample", "parquet");
    }
}

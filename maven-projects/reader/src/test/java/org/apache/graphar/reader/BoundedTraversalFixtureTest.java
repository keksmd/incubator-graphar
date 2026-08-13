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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.graphar.info.loader.impl.LocalFileSystemStringGraphInfoLoader;
import org.apache.graphar.io.PhysicalReader;
import org.apache.graphar.io.parquet.ParquetPhysicalReader;
import org.apache.graphar.storage.local.LocalStorage;
import org.junit.Test;

public class BoundedTraversalFixtureTest {
    private static final long MAX_VERTICES = 10_000L;
    private static final long MAX_EDGES = 100_000L;
    private static final long BENCHMARK_VERTEX = 297L;

    @Test
    public void reproducesTheDocumentedOutgoingFrontierSizes() throws Exception {
        CsrGraph csr = csr(CsrDirection.OUTGOING);

        TraversalResult result =
                BoundedTraversal.neighborhood(csr, BENCHMARK_VERTEX, 3, Integer.MAX_VALUE);

        assertFalse(result.truncated());
        assertEquals(1, result.frontierSize(0));
        assertEquals(53, result.frontierSize(1));
        assertEquals(181, result.frontierSize(2));
        assertEquals(93, result.frontierSize(3));
        assertEquals(328, result.size());

        List<Integer> benchmarkSizes = new ArrayList<>();
        java.util.Set<Long> benchmarkReached = new java.util.TreeSet<>();
        java.util.Set<Long> frontier = new java.util.LinkedHashSet<>(List.of(BENCHMARK_VERTEX));
        benchmarkSizes.add(frontier.size());
        benchmarkReached.addAll(frontier);
        for (int hop = 0; hop < 3; hop++) {
            java.util.Set<Long> next = new java.util.LinkedHashSet<>();
            for (long vertex : frontier) {
                for (long neighbor : adjacency(csr, vertex)) {
                    next.add(neighbor);
                }
            }
            frontier = next;
            benchmarkReached.addAll(frontier);
            benchmarkSizes.add(frontier.size());
        }
        assertEquals(List.of(1, 53, 219, 311), benchmarkSizes);
        assertArrayEquals(
                benchmarkReached.stream().mapToLong(Long::longValue).toArray(),
                Arrays.stream(result.vertices()).sorted().toArray());
        assertEquals(BENCHMARK_VERTEX, result.vertices()[0]);
    }

    @Test
    public void everyReachedVertexMatchesAnIndependentBreadthFirstExpansion() throws Exception {
        CsrGraph csr = csr(CsrDirection.UNDIRECTED);

        TraversalResult result =
                BoundedTraversal.neighborhood(csr, BENCHMARK_VERTEX, 2, Integer.MAX_VALUE);

        List<Long> oracle = oracleExpansion(csr, BENCHMARK_VERTEX, 2);
        assertArrayEquals(
                oracle.stream().mapToLong(Long::longValue).sorted().toArray(),
                Arrays.stream(result.vertices()).sorted().toArray());
        for (int position = 0; position < result.size(); position++) {
            assertTrue(result.depths()[position] <= 2);
        }
    }

    @Test
    public void undirectedExpansionReachesAtLeastAsMuchAsTheOutgoingOne() throws Exception {
        TraversalResult outgoing =
                BoundedTraversal.neighborhood(
                        csr(CsrDirection.OUTGOING), BENCHMARK_VERTEX, 2, Integer.MAX_VALUE);
        TraversalResult undirected =
                BoundedTraversal.neighborhood(
                        csr(CsrDirection.UNDIRECTED), BENCHMARK_VERTEX, 2, Integer.MAX_VALUE);

        List<Long> both = boxed(undirected.vertices());
        for (long vertex : outgoing.vertices()) {
            assertTrue("missing " + vertex, both.contains(vertex));
        }
        assertTrue(undirected.size() > outgoing.size());
    }

    @Test
    public void theNodeBudgetTruncatesAndKeepsThePrefixOfTheUnboundedExpansion() throws Exception {
        CsrGraph csr = csr(CsrDirection.UNDIRECTED);

        TraversalResult unbounded =
                BoundedTraversal.neighborhood(csr, BENCHMARK_VERTEX, 4, Integer.MAX_VALUE);
        TraversalResult bounded = BoundedTraversal.neighborhood(csr, BENCHMARK_VERTEX, 4, 50);

        assertTrue(unbounded.size() > 50);
        assertTrue(bounded.truncated());
        assertEquals(50, bounded.size());
        assertArrayEquals(Arrays.copyOf(unbounded.vertices(), 50), bounded.vertices());
        assertArrayEquals(Arrays.copyOf(unbounded.depths(), 50), bounded.depths());
    }

    @Test
    public void reportsOneComponentIdentifierFromEveryMemberOfThatComponent() throws Exception {
        CsrGraph csr = csr(CsrDirection.UNDIRECTED);

        TraversalResult component = BoundedTraversal.component(csr, BENCHMARK_VERTEX, 5_000);
        assertFalse(component.truncated());

        long componentId = component.componentId();
        for (long member :
                new long[] {component.vertices()[1], component.vertices()[component.size() - 1]}) {
            TraversalResult fromMember = BoundedTraversal.component(csr, member, 5_000);
            assertFalse(fromMember.truncated());
            assertEquals(component.size(), fromMember.size());
            assertEquals(componentId, fromMember.componentId());
        }
    }

    @Test
    public void aZeroDepthExpansionReturnsOnlyTheStartVertex() throws Exception {
        TraversalResult result =
                BoundedTraversal.neighborhood(csr(CsrDirection.UNDIRECTED), 0L, 0, 50);

        assertArrayEquals(new long[] {0L}, result.vertices());
        assertArrayEquals(new int[] {0}, result.depths());
        assertFalse(result.truncated());
    }

    @Test
    public void rejectsBudgetsAndStartVerticesThatCannotBeServed() throws Exception {
        CsrGraph csr = csr(CsrDirection.UNDIRECTED);

        assertThrows(
                IllegalArgumentException.class,
                () -> BoundedTraversal.neighborhood(csr, 0L, -1, 50));
        assertThrows(
                IllegalArgumentException.class, () -> BoundedTraversal.neighborhood(csr, 0L, 2, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> BoundedTraversal.neighborhood(csr, csr.vertexCount(), 2, 50));
    }

    private static long[] adjacency(CsrGraph csr, long vertex) {
        long[] offsets = csr.offsets();
        return Arrays.copyOfRange(
                csr.destinations(),
                Math.toIntExact(offsets[Math.toIntExact(vertex)]),
                Math.toIntExact(offsets[Math.toIntExact(vertex) + 1]));
    }

    private static List<Long> oracleExpansion(CsrGraph csr, long start, int maxDepth) {
        long[] offsets = csr.offsets();
        long[] destinations = csr.destinations();
        List<Long> reached = new ArrayList<>();
        List<Long> frontier = new ArrayList<>(List.of(start));
        reached.add(start);
        for (int depth = 0; depth < maxDepth; depth++) {
            List<Long> next = new ArrayList<>();
            for (long vertex : frontier) {
                int from = Math.toIntExact(offsets[Math.toIntExact(vertex)]);
                int to = Math.toIntExact(offsets[Math.toIntExact(vertex) + 1]);
                for (int entry = from; entry < to; entry++) {
                    long neighbor = destinations[entry];
                    if (!reached.contains(neighbor)) {
                        reached.add(neighbor);
                        next.add(neighbor);
                    }
                }
            }
            frontier = next;
        }
        return reached;
    }

    private static List<Long> boxed(long[] values) {
        List<Long> boxed = new ArrayList<>(values.length);
        for (long value : values) {
            boxed.add(value);
        }
        return boxed;
    }

    private static CsrGraph csr(CsrDirection direction) throws IOException {
        return openGraph()
                .edge("person", "knows", "person")
                .materializeCsr(MAX_VERTICES, MAX_EDGES, direction);
    }

    private static GraphReader openGraph() throws IOException {
        PhysicalReader physicalReader = new ParquetPhysicalReader(new LocalStorage());
        return GraphReader.open(
                fixturePath().resolve("ldbc_sample.graph.yml").toUri(),
                new LocalFileSystemStringGraphInfoLoader(),
                new LocalStorage(),
                physicalReader);
    }

    private static Path fixturePath() {
        return Path.of("..", "..", "testing", "ldbc_sample", "parquet");
    }
}

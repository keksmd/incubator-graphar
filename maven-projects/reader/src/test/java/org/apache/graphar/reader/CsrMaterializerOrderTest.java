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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.Test;

/**
 * Pins that the placement strategy is invisible in the result.
 *
 * <p>A sorted topology takes a different path through the materializer than an unsorted one, so the
 * two have to be proven to agree. They are given the same edge multiset in different orders, and
 * every offset and every entry of the two projections is compared.
 */
public class CsrMaterializerOrderTest {
    private static final int VERTEX_COUNT = 4_000;
    private static final int EDGE_COUNT = 20_000;
    private static final long SEED = 20260813L;

    @Test
    public void aSortedTopologyProducesTheSameProjectionAsAnUnsortedOne() throws IOException {
        for (CsrDirection direction : CsrDirection.values()) {
            int[][] edges = randomEdges();
            int[][] shuffled = shuffle(edges);
            int[][] sorted = sortBySourceThenTarget(edges);

            CsrGraph fromShuffled = build(shuffled, direction);
            CsrGraph fromSorted = build(sorted, direction);

            assertArrayEquals(direction + " offsets", fromShuffled.offsets(), fromSorted.offsets());
            assertArrayEquals(
                    direction + " entries", fromShuffled.destinations(), fromSorted.destinations());
        }
    }

    @Test
    public void aTopologySortedBySourceAloneProducesTheSameProjection() throws IOException {
        for (CsrDirection direction : CsrDirection.values()) {
            int[][] edges = randomEdges();
            int[][] shuffled = shuffle(edges);
            int[][] descending = sortBySourceThenDescendingTarget(edges);

            CsrGraph fromShuffled = build(shuffled, direction);
            CsrGraph fromDescending = build(descending, direction);

            assertArrayEquals(
                    direction + " offsets", fromShuffled.offsets(), fromDescending.offsets());
            assertArrayEquals(
                    direction + " entries",
                    fromShuffled.destinations(),
                    fromDescending.destinations());
        }
    }

    @Test
    public void aHubVertexIsOrderedTheSameWhicheverPathBuiltIt() throws IOException {
        List<int[]> edges = new ArrayList<>();
        for (int vertex = 1; vertex < 500; vertex++) {
            edges.add(new int[] {0, vertex});
            edges.add(new int[] {vertex, 0});
        }
        edges.add(new int[] {0, 0});
        int[][] all = edges.toArray(new int[0][]);

        CsrGraph fromShuffled = build(shuffle(all), CsrDirection.UNDIRECTED);
        CsrGraph fromSorted = build(sortBySourceThenTarget(all), CsrDirection.UNDIRECTED);

        assertArrayEquals(fromShuffled.offsets(), fromSorted.offsets());
        assertArrayEquals(fromShuffled.destinations(), fromSorted.destinations());
    }

    private static CsrGraph build(int[][] edges, CsrDirection direction) throws IOException {
        int[] sources = new int[edges.length];
        int[] targets = new int[edges.length];
        for (int edge = 0; edge < edges.length; edge++) {
            sources[edge] = edges[edge][0];
            targets[edge] = edges[edge][1];
        }
        long vertexCount = VERTEX_COUNT;
        for (int[] edge : edges) {
            vertexCount = Math.max(vertexCount, Math.max(edge[0], edge[1]) + 1L);
        }
        return CsrMaterializer.fromEndpoints(
                sources, targets, edges.length, vertexCount, direction);
    }

    private static int[][] randomEdges() {
        Random random = new Random(SEED);
        int[][] edges = new int[EDGE_COUNT][];
        for (int edge = 0; edge < EDGE_COUNT; edge++) {
            edges[edge] = new int[] {random.nextInt(VERTEX_COUNT), random.nextInt(VERTEX_COUNT)};
        }
        return edges;
    }

    private static int[][] shuffle(int[][] edges) {
        List<int[]> copy = new ArrayList<>();
        for (int[] edge : edges) {
            copy.add(edge.clone());
        }
        Collections.shuffle(copy, new Random(SEED + 1L));
        return copy.toArray(new int[0][]);
    }

    private static int[][] sortBySourceThenTarget(int[][] edges) {
        int[][] copy = copyOf(edges);
        Arrays.sort(
                copy,
                (left, right) -> left[0] != right[0] ? left[0] - right[0] : left[1] - right[1]);
        return copy;
    }

    private static int[][] sortBySourceThenDescendingTarget(int[][] edges) {
        int[][] copy = copyOf(edges);
        Arrays.sort(
                copy,
                (left, right) -> left[0] != right[0] ? left[0] - right[0] : right[1] - left[1]);
        return copy;
    }

    private static int[][] copyOf(int[][] edges) {
        int[][] copy = new int[edges.length][];
        for (int edge = 0; edge < edges.length; edge++) {
            copy[edge] = edges[edge].clone();
        }
        return copy;
    }
}

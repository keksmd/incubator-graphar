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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;
import org.apache.graphar.reader.CsrDirection;
import org.apache.graphar.reader.HeterogeneousCsr;
import org.junit.Test;

/**
 * Proves a view answers as the projection that would result from folding the delta into the base.
 *
 * <p>A freshness layer is only usable if reading through it is indistinguishable from reading a
 * projection that already contains the patches, so the cases here compare against a merged
 * projection rather than against a hand-written expectation.
 */
public class GraphViewTest {
    private static final long SEED = 20260821L;

    @Test
    public void aViewAnswersWhatAMergedProjectionAnswers() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        int count = 2_000;
        long[] sources = new long[count];
        long[] targets = new long[count];
        Random random = new Random(SEED);
        for (int edge = 0; edge < count; edge++) {
            sources[edge] = random.nextInt((int) base.vertexCount());
            targets[edge] = random.nextInt((int) base.vertexCount());
        }
        MutableDelta delta = MutableDelta.on(base);
        delta.appendAll(sources, targets, count);
        GraphView view = delta.seal();

        HeterogeneousCsr merged = base.merge(sources, targets, count);

        assertEquals(merged.vertexCount(), view.vertexCount());
        for (long vertex = 0; vertex < merged.vertexCount(); vertex++) {
            assertArrayEquals("vertex " + vertex, merged.neighbors(vertex), view.neighbors(vertex));
            assertEquals("vertex " + vertex, merged.neighbors(vertex).length, view.degree(vertex));
        }
    }

    @Test
    public void aDirectedViewAnswersWhatADirectedMergeAnswers() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection(CsrDirection.OUTGOING);
        long first = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        long second = DeltaFixture.unrelated(base, first);
        MutableDelta delta = MutableDelta.on(base);
        delta.append(first, second);
        GraphView view = delta.seal();

        HeterogeneousCsr merged = base.merge(new long[] {first}, new long[] {second}, 1);

        assertArrayEquals(merged.neighbors(first), view.neighbors(first));
        assertArrayEquals(
                "an outgoing delta does not patch the target",
                merged.neighbors(second),
                view.neighbors(second));
    }

    @Test
    public void aViewRefusesABaseItsDeltaWasNotTakenAgainst() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        MutableDelta delta = MutableDelta.on(base);
        DeltaSnapshot snapshot = delta.seal().delta();

        HeterogeneousCsr grown =
                base.merge(
                        HeterogeneousCsr.batch().addVertex(DeltaFixture.PERSON, "99000000000001"));

        assertTrue(grown.vertexCount() > base.vertexCount());
        assertThrows(IllegalArgumentException.class, () -> GraphView.of(grown, snapshot));
        assertThrows(
                IllegalArgumentException.class,
                () -> GraphView.of(DeltaFixture.projection(CsrDirection.OUTGOING), snapshot));
    }

    @Test
    public void aTraversalCrossesFromTheBaseIntoTheDelta() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long start = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        MutableDelta delta = MutableDelta.on(base);
        long neighbor = base.neighbors(start)[0];
        long arriving = delta.vertexId(DeltaFixture.PERSON, "99000000000001");
        delta.append(neighbor, arriving);
        GraphView view = delta.seal();

        ViewTraversalResult reached = view.neighborhood(start, 2, 1_000);

        assertFalse(reached.truncated());
        assertTrue(
                "the vertex the delta introduced is two hops away",
                contains(reached.vertices(), arriving));
        assertEquals(2, reached.depths()[indexOf(reached.vertices(), arriving)]);
    }

    @Test
    public void aTraversalStopsAtItsNodeBudget() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long start = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        MutableDelta delta = MutableDelta.on(base);
        GraphView view = delta.seal();

        ViewTraversalResult reached = view.component(start, 3);

        assertTrue(reached.truncated());
        assertEquals(3, reached.size());
    }

    @Test
    public void aTraversalOverAViewReachesWhatATraversalOverTheMergeReaches() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long start = base.globalIndex(DeltaFixture.PERSON, DeltaFixture.PERSON_ID);
        int count = 500;
        long[] sources = new long[count];
        long[] targets = new long[count];
        Random random = new Random(SEED);
        for (int edge = 0; edge < count; edge++) {
            sources[edge] = random.nextInt((int) base.vertexCount());
            targets[edge] = random.nextInt((int) base.vertexCount());
        }
        MutableDelta delta = MutableDelta.on(base);
        delta.appendAll(sources, targets, count);
        GraphView view = delta.seal();
        HeterogeneousCsr merged = base.merge(sources, targets, count);

        ViewTraversalResult overView = view.neighborhood(start, 2, 10_000);
        long[] overMerge = expand(merged, start, 2);

        assertFalse(overView.truncated());
        long[] reached = overView.vertices();
        Arrays.sort(reached);
        assertArrayEquals(overMerge, reached);
    }

    private static long[] expand(HeterogeneousCsr projection, long start, int maxDepth) {
        boolean[] seen = new boolean[(int) projection.vertexCount()];
        long[] frontier = new long[] {start};
        seen[(int) start] = true;
        for (int depth = 0; depth < maxDepth; depth++) {
            long[] next = new long[0];
            for (long vertex : frontier) {
                for (long neighbor : projection.neighbors(vertex)) {
                    if (!seen[(int) neighbor]) {
                        seen[(int) neighbor] = true;
                        next = Arrays.copyOf(next, next.length + 1);
                        next[next.length - 1] = neighbor;
                    }
                }
            }
            frontier = next;
        }
        int size = 0;
        for (boolean reached : seen) {
            if (reached) {
                size++;
            }
        }
        long[] reached = new long[size];
        int position = 0;
        for (int vertex = 0; vertex < seen.length; vertex++) {
            if (seen[vertex]) {
                reached[position++] = vertex;
            }
        }
        return reached;
    }

    private static boolean contains(long[] values, long value) {
        return indexOf(values, value) >= 0;
    }

    private static int indexOf(long[] values, long value) {
        for (int position = 0; position < values.length; position++) {
            if (values[position] == value) {
                return position;
            }
        }
        return -1;
    }
}

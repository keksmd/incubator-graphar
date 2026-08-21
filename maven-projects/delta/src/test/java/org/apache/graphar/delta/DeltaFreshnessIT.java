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
import static org.junit.Assert.assertTrue;

import java.util.Random;
import org.apache.graphar.reader.HeterogeneousCsr;
import org.junit.Test;

/**
 * Measures what the delta exists for: publishing a patch must cost far less than folding it into
 * the projection, and reading through the published patch must cost a bounded amount more.
 *
 * <p>Both numbers are the whole argument for a mutable layer. If publishing were as expensive as a
 * merge, the projection alone would do; if reading amplified without a bound, the layer would trade
 * a freshness problem for a latency one. This runs as an integration test because it times work
 * rather than checking a value.
 */
public class DeltaFreshnessIT {
    private static final long SEED = 20260821L;
    private static final int ROUNDS = 20;
    private static final int PER_ROUND = 200;
    private static final int BASE_EDGES = 400_000;

    @Test
    public void publishingAPatchCostsFarLessThanFoldingItIntoTheProjection() throws Exception {
        HeterogeneousCsr base = dense();
        int vertices = (int) base.vertexCount();
        Random random = new Random(SEED);
        long[] sources = new long[PER_ROUND];
        long[] targets = new long[PER_ROUND];

        warmUp(base, vertices);

        MutableDelta delta = MutableDelta.on(base);
        long deltaNanos = 0;
        for (int round = 0; round < ROUNDS; round++) {
            fill(random, sources, targets, vertices);
            long started = System.nanoTime();
            delta.appendAll(sources, targets, PER_ROUND);
            delta.seal();
            deltaNanos += System.nanoTime() - started;
        }

        random = new Random(SEED);
        HeterogeneousCsr merged = base;
        long mergeNanos = 0;
        for (int round = 0; round < ROUNDS; round++) {
            fill(random, sources, targets, vertices);
            long started = System.nanoTime();
            merged = merged.merge(sources, targets, PER_ROUND);
            mergeNanos += System.nanoTime() - started;
        }

        GraphView view = delta.current();
        for (long vertex = 0; vertex < merged.vertexCount(); vertex++) {
            assertArrayEquals("vertex " + vertex, merged.neighbors(vertex), view.neighbors(vertex));
        }
        assertEquals((long) ROUNDS * PER_ROUND, view.deltaEdgeCount());
        assertTrue(
                "publishing "
                        + ROUNDS
                        + " patches took "
                        + deltaNanos / 1_000_000
                        + " ms against "
                        + mergeNanos / 1_000_000
                        + " ms of merging",
                deltaNanos * 10 < mergeNanos);
    }

    private static HeterogeneousCsr dense() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        long[] sources = new long[BASE_EDGES];
        long[] targets = new long[BASE_EDGES];
        fill(new Random(SEED), sources, targets, (int) base.vertexCount());
        return base.merge(sources, targets, BASE_EDGES);
    }

    private static void warmUp(HeterogeneousCsr base, int vertices) throws Exception {
        long[] sources = new long[PER_ROUND];
        long[] targets = new long[PER_ROUND];
        Random random = new Random(SEED + 1);
        MutableDelta delta = MutableDelta.on(base);
        HeterogeneousCsr merged = base;
        for (int round = 0; round < 3; round++) {
            fill(random, sources, targets, vertices);
            delta.appendAll(sources, targets, PER_ROUND);
            delta.seal();
            merged = merged.merge(sources, targets, PER_ROUND);
        }
        merged.neighbors(0);
        delta.current().neighbors(0);
    }

    @Test
    public void readAmplificationStaysWithinTheDeltaBudget() throws Exception {
        HeterogeneousCsr base = DeltaFixture.projection();
        int vertices = (int) base.vertexCount();
        Random random = new Random(SEED);
        long[] sources = new long[ROUNDS * PER_ROUND];
        long[] targets = new long[ROUNDS * PER_ROUND];
        fill(random, sources, targets, vertices);
        MutableDelta delta = MutableDelta.on(base);
        delta.appendAll(sources, targets, sources.length);
        GraphView view = delta.seal();

        long patched = 0;
        long extra = 0;
        for (long vertex = 0; vertex < view.vertexCount(); vertex++) {
            int degree = view.delta().degree(vertex);
            if (degree > 0) {
                patched++;
                extra += degree;
            }
        }

        assertEquals(view.deltaEdgeCount() * 2L, extra);
        assertTrue(
                "every patch a read may walk is bounded by the ceiling",
                extra <= DeltaOptions.defaults().maxEdges() * 2L);
        assertEquals(patched, view.delta().patchedVertexCount());
    }

    private static void fill(Random random, long[] sources, long[] targets, int vertices) {
        for (int edge = 0; edge < sources.length; edge++) {
            sources[edge] = random.nextInt(vertices);
            targets[edge] = random.nextInt(vertices);
        }
    }
}

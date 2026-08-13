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

import java.util.Random;
import org.junit.Test;

/**
 * Opt-in volume run. Set {@code GRAPHAR_SCALE_VERTICES} and {@code GRAPHAR_SCALE_EDGES} and give
 * the JVM a heap that fits the reported peak, for example {@code -DargLine=-Xmx6g}.
 *
 * <p>The fixtures are thousands of rows, so nothing else in the suite says what the projection does
 * at the size a production identity graph reaches. This builds a synthetic graph of that size and
 * reports build time, resident size, and the latency of the bounded expansions the serving contract
 * is written against. It asserts only the contract bounds, because throughput depends on the host.
 */
public class CsrScaleIT {
    private static final long DEFAULT_VERTICES = 10_000_000L;
    private static final long DEFAULT_EDGES = 30_000_000L;
    private static final int SAMPLE_REQUESTS = 200;
    private static final int MAX_DEPTH = 4;
    private static final int MAX_NODES = 50;
    private static final long RESPONSE_BUDGET_MILLIS = 3_000L;
    private static final long SEED = 20260813L;

    @Test
    public void answersBoundedRequestsWithinTheContractAtVolume() {
        long vertexCount = size("GRAPHAR_SCALE_VERTICES", DEFAULT_VERTICES);
        long edgeCount = size("GRAPHAR_SCALE_EDGES", DEFAULT_EDGES);
        long peak =
                ProjectionCapacity.peakBuildBytes(vertexCount, edgeCount, CsrDirection.UNDIRECTED);
        long serving =
                ProjectionCapacity.heapBytes(
                        vertexCount,
                        ProjectionCapacity.entryCount(edgeCount, CsrDirection.UNDIRECTED));
        System.out.println(
                "scale: vertices="
                        + vertexCount
                        + " edges="
                        + edgeCount
                        + " servingBytes="
                        + serving
                        + " peakBuildBytes="
                        + peak
                        + " maxHeapBytes="
                        + Runtime.getRuntime().maxMemory());

        int edges = Math.toIntExact(edgeCount);
        int[] sources = new int[edges];
        int[] targets = new int[edges];
        Random random = new Random(SEED);
        for (int edge = 0; edge < edges; edge++) {
            long source = Math.floorMod(random.nextLong(), vertexCount);
            long target = Math.floorMod(random.nextLong(), vertexCount);
            sources[edge] = (int) source;
            targets[edge] = (int) (source == target ? (target + 1) % vertexCount : target);
        }

        long buildStart = System.nanoTime();
        CsrGraph csr;
        try {
            csr =
                    CsrMaterializer.fromEndpoints(
                            sources, targets, edges, vertexCount, CsrDirection.UNDIRECTED);
        } catch (java.io.IOException failure) {
            throw new AssertionError(failure);
        }
        long buildMillis = (System.nanoTime() - buildStart) / 1_000_000L;
        sources = null;
        targets = null;

        assertEquals(vertexCount, csr.vertexCount());
        assertEquals(edgeCount * 2L, csr.edgeCount());

        long[] starts = new long[SAMPLE_REQUESTS];
        for (int request = 0; request < SAMPLE_REQUESTS; request++) {
            starts[request] = Math.floorMod(random.nextLong(), vertexCount);
        }
        for (long start : starts) {
            BoundedTraversal.neighborhood(csr, start, MAX_DEPTH, MAX_NODES);
        }

        long slowestMillis = 0;
        long totalNanos = 0;
        for (long start : starts) {
            long began = System.nanoTime();
            TraversalResult result =
                    BoundedTraversal.neighborhood(csr, start, MAX_DEPTH, MAX_NODES);
            long elapsed = System.nanoTime() - began;
            totalNanos += elapsed;
            slowestMillis = Math.max(slowestMillis, elapsed / 1_000_000L);
            assertTrue(result.size() >= 1);
            assertTrue(result.size() <= MAX_NODES);
        }

        long componentStart = System.nanoTime();
        TraversalResult component = BoundedTraversal.component(csr, starts[0], 100_000);
        long componentMillis = (System.nanoTime() - componentStart) / 1_000_000L;

        System.out.println(
                "scale: buildMillis="
                        + buildMillis
                        + " neighborsMeanMicros="
                        + totalNanos / SAMPLE_REQUESTS / 1_000L
                        + " neighborsSlowestMillis="
                        + slowestMillis
                        + " componentMillis="
                        + componentMillis
                        + " componentSize="
                        + component.size()
                        + " componentTruncated="
                        + component.truncated());

        assertTrue(
                "a bounded neighborhood must stay inside the response budget: " + slowestMillis,
                slowestMillis < RESPONSE_BUDGET_MILLIS);
        assertTrue(
                "a bounded component must stay inside the response budget: " + componentMillis,
                componentMillis < RESPONSE_BUDGET_MILLIS);
    }

    private static long size(String variable, long fallback) {
        String configured = System.getenv(variable);
        return configured == null ? fallback : Long.parseLong(configured);
    }
}

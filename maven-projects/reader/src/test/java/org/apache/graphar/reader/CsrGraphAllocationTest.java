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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import org.junit.Test;

/**
 * Bounds the allocation cost of serving from a materialized CSR.
 *
 * <p>A serving request must not pay for the size of the graph. The bulk accessors hand out
 * defensive copies, so a traversal that exported them would allocate the whole topology on every
 * call; these tests measure thread allocation around a batch of calls and fail if that happens
 * again. Allocation is measured rather than elapsed time so the bound is decided by the code path
 * and not by the load on the machine.
 */
public class CsrGraphAllocationTest {
    private static final int VERTEX_COUNT = 500_000;
    private static final int DEGREE = 4;
    private static final long TOPOLOGY_BYTES =
            (long) Long.BYTES * ((long) VERTEX_COUNT * DEGREE + VERTEX_COUNT + 1L);

    @Test
    public void aTraversalDoesNotAllocateTheTopologyItReads() {
        com.sun.management.ThreadMXBean bean = threadBean();
        CsrGraph csr = ring();

        for (int start = 0; start < 4; start++) {
            BoundedTraversal.neighborhood(csr, start, 2, 50);
        }

        long before = bean.getThreadAllocatedBytes(Thread.currentThread().getId());
        int calls = 20;
        for (int start = 0; start < calls; start++) {
            TraversalResult result = BoundedTraversal.neighborhood(csr, start, 2, 50);
            assertTrue(result.size() > 1);
        }
        long allocated = bean.getThreadAllocatedBytes(Thread.currentThread().getId()) - before;

        assertTrue(
                "a batch of " + calls + " traversals allocated " + allocated + " bytes",
                allocated < TOPOLOGY_BYTES);
    }

    @Test
    public void aBoundedTraversalCostsTheNodeBudgetAndNotTheVertexCount() {
        com.sun.management.ThreadMXBean bean = threadBean();
        CsrGraph csr = ring();
        int maxNodes = 50;

        for (int start = 0; start < 4; start++) {
            BoundedTraversal.neighborhood(csr, start, 4, maxNodes);
        }

        long before = bean.getThreadAllocatedBytes(Thread.currentThread().getId());
        int calls = 100;
        for (int start = 0; start < calls; start++) {
            assertTrue(BoundedTraversal.neighborhood(csr, start, 4, maxNodes).size() > 1);
        }
        long perCall =
                (bean.getThreadAllocatedBytes(Thread.currentThread().getId()) - before) / calls;

        assertTrue(
                "a request bounded to "
                        + maxNodes
                        + " nodes allocated "
                        + perCall
                        + " bytes on a graph of "
                        + VERTEX_COUNT
                        + " vertices",
                perCall < 8L * 1024L);
    }

    @Test
    public void iteratingANeighbourhoodAllocatesNothing() {
        com.sun.management.ThreadMXBean bean = threadBean();
        CsrGraph csr = ring();

        long warmup = 0;
        for (int vertex = 0; vertex < 1_000; vertex++) {
            warmup += csr.degree(vertex);
        }
        assertTrue(warmup > 0);

        long before = bean.getThreadAllocatedBytes(Thread.currentThread().getId());
        long sum = 0;
        for (int vertex = 0; vertex < 100_000; vertex++) {
            long degree = csr.degree(vertex);
            for (long position = 0; position < degree; position++) {
                sum += csr.neighbor(vertex, position);
            }
        }
        long allocated = bean.getThreadAllocatedBytes(Thread.currentThread().getId()) - before;

        assertTrue(sum > 0);
        assertTrue("iterating allocated " + allocated + " bytes", allocated < 64L * 1024L);
    }

    @Test
    public void theZeroCopyAccessorsAgreeWithTheBulkExport() {
        CsrGraph csr = ring();
        long[] offsets = csr.offsets();
        long[] destinations = csr.destinations();

        for (long vertex : new long[] {0L, 1L, 7L, VERTEX_COUNT - 1L}) {
            int index = Math.toIntExact(vertex);
            int from = Math.toIntExact(offsets[index]);
            int to = Math.toIntExact(offsets[index + 1]);
            assertEquals(to - from, csr.degree(vertex));
            long[] expected = Arrays.copyOfRange(destinations, from, to);
            long[] actual = new long[expected.length];
            for (int position = 0; position < expected.length; position++) {
                actual[position] = csr.neighbor(vertex, position);
            }
            assertArrayEquals(expected, actual);
        }
    }

    @Test
    public void theBulkExportStaysDefensive() {
        CsrGraph csr = ring();

        long[] destinations = csr.destinations();
        long original = destinations[0];
        destinations[0] = -1L;

        assertEquals(original, csr.destinations()[0]);
        assertEquals(original, csr.neighbor(0L, 0L));
    }

    @Test
    public void theZeroCopyAccessorsRejectOutOfRangeRequests() {
        CsrGraph csr = ring();

        assertThrows(IllegalArgumentException.class, () -> csr.degree(-1L));
        assertThrows(IllegalArgumentException.class, () -> csr.degree(VERTEX_COUNT));
        assertThrows(IllegalArgumentException.class, () -> csr.neighbor(0L, -1L));
        assertThrows(IllegalArgumentException.class, () -> csr.neighbor(0L, DEGREE));
    }

    private static com.sun.management.ThreadMXBean threadBean() {
        java.lang.management.ThreadMXBean platform = ManagementFactory.getThreadMXBean();
        assumeTrue(platform instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean bean = (com.sun.management.ThreadMXBean) platform;
        assumeTrue(bean.isThreadAllocatedMemorySupported());
        bean.setThreadAllocatedMemoryEnabled(true);
        return bean;
    }

    private static CsrGraph ring() {
        long[] offsets = new long[VERTEX_COUNT + 1];
        long[] destinations = new long[VERTEX_COUNT * DEGREE];
        for (int vertex = 0; vertex < VERTEX_COUNT; vertex++) {
            offsets[vertex + 1] = offsets[vertex] + DEGREE;
            for (int step = 0; step < DEGREE; step++) {
                destinations[vertex * DEGREE + step] = (vertex + step + 1L) % VERTEX_COUNT;
            }
        }
        return new CsrGraph(offsets, destinations);
    }
}

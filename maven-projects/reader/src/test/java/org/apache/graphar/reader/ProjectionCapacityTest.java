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
import static org.junit.Assert.fail;

import org.junit.Test;

/** Pins the size a heap CSR admits and the message a caller gets when a graph is past it. */
public class ProjectionCapacityTest {
    private static final long BILLION = 1_000_000_000L;

    @Test
    public void anUndirectedProjectionStoresEveryEdgeTwice() {
        assertEquals(200L, ProjectionCapacity.entryCount(100L, CsrDirection.UNDIRECTED));
        assertEquals(100L, ProjectionCapacity.entryCount(100L, CsrDirection.OUTGOING));
        assertEquals(100L, ProjectionCapacity.entryCount(100L, CsrDirection.INCOMING));
    }

    @Test
    public void theHeapCostOfAProjectionIsFourBytesPerSlot() {
        assertEquals(
                4L * (1_000L + 1L) + 4L * 4_000L, ProjectionCapacity.heapBytes(1_000L, 4_000L));
    }

    @Test
    public void anIdentityGraphSizedProjectionIsReportedInGigabytes() {
        long vertices = 50L * 1_000_000L;
        long edges = 200L * 1_000_000L;
        long serving =
                ProjectionCapacity.heapBytes(
                        vertices, ProjectionCapacity.entryCount(edges, CsrDirection.UNDIRECTED));
        long peak = ProjectionCapacity.peakBuildBytes(vertices, edges, CsrDirection.UNDIRECTED);

        assertEquals(1_800_000_004L, serving);
        assertEquals(3_600_000_008L, peak);
        assertTrue("a build peaks above what it settles at", peak > serving);
    }

    @Test
    public void aGraphPastTheArrayIndexIsRejectedByName() {
        try {
            ProjectionCapacity.requireAddressable(3L * BILLION, 10L);
            fail("a graph of three billion vertices must not be accepted");
        } catch (ProjectionTooLargeException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("3000000000"));
            assertTrue(
                    expected.getMessage(),
                    expected.getMessage()
                            .contains(String.valueOf(ProjectionCapacity.MAX_VERTICES)));
        }
        try {
            ProjectionCapacity.requireAddressable(10L, 3L * BILLION);
            fail("three billion adjacency entries must not be accepted");
        } catch (ProjectionTooLargeException expected) {
            assertTrue(
                    expected.getMessage(),
                    expected.getMessage().contains(String.valueOf(ProjectionCapacity.MAX_ENTRIES)));
        }
    }

    @Test
    public void theLargestAddressableGraphIsStillAccepted() {
        ProjectionCapacity.requireAddressable(
                ProjectionCapacity.MAX_VERTICES, ProjectionCapacity.MAX_ENTRIES);
    }

    @Test
    public void aBuildThatWouldNotFitIsRejectedBeforeItAllocates() {
        try {
            ProjectionCapacity.requireHeadroom(
                    10L * 1_000_000L,
                    80L * 1_000_000L,
                    CsrDirection.UNDIRECTED,
                    256L * 1024 * 1024);
            fail("a build needing more than the heap must not be attempted");
        } catch (ProjectionTooLargeException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("268435456"));
            assertTrue(expected.getMessage(), expected.getMessage().contains("Raise the heap"));
        }
    }

    @Test
    public void aBuildThatFitsIsNotRejected() {
        ProjectionCapacity.requireHeadroom(
                1_000L, 4_000L, CsrDirection.UNDIRECTED, 64L * 1024 * 1024);
    }
}

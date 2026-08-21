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

package org.apache.graphar.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.type.AdjListType;
import org.junit.Test;

public class AdjacencyOrderingTest {
    private final EdgeInfo edgeInfo = EdgeInfoFixtures.personKnowsPerson(1024, 100, 50);

    @Test
    public void takesTheSourceChunkSizeForASourceOrderedLayout() {
        AdjacencyOrdering ordering = AdjacencyOrdering.of(edgeInfo, AdjListType.ordered_by_source);

        assertEquals(AdjListType.ordered_by_source, ordering.adjListType());
        assertEquals(100, ordering.vertexChunkSize());
        assertEquals(1024, ordering.edgeChunkSize());
    }

    @Test
    public void takesTheDestinationChunkSizeForADestinationOrderedLayout() {
        AdjacencyOrdering ordering = AdjacencyOrdering.of(edgeInfo, AdjListType.ordered_by_dest);

        assertEquals(50, ordering.vertexChunkSize());
        assertEquals(1024, ordering.edgeChunkSize());
    }

    @Test
    public void namesTheOwningEndpointPrimaryInEachLayout() {
        AdjacencyOrdering bySource = AdjacencyOrdering.of(edgeInfo, AdjListType.ordered_by_source);
        AdjacencyOrdering byDest = AdjacencyOrdering.of(edgeInfo, AdjListType.ordered_by_dest);

        assertEquals(7, bySource.primaryVertex(7, 11));
        assertEquals(11, bySource.secondaryVertex(7, 11));
        assertEquals(11, byDest.primaryVertex(7, 11));
        assertEquals(7, byDest.secondaryVertex(7, 11));
    }

    @Test
    public void addressesAPrimaryVertexThroughItsOwnChunkSize() {
        assertEquals(
                2, AdjacencyOrdering.of(edgeInfo, AdjListType.ordered_by_source).vertexChunk(297));
        assertEquals(
                5, AdjacencyOrdering.of(edgeInfo, AdjListType.ordered_by_dest).vertexChunk(297));
    }

    @Test
    public void refusesAnUnorderedLayout() {
        assertThrows(
                IllegalArgumentException.class,
                () -> AdjacencyOrdering.of(edgeInfo, AdjListType.unordered_by_source));
    }

    @Test
    public void refusesALayoutTheEdgeTypeDoesNotDeclare() {
        EdgeInfo sourceOnly = EdgeInfoFixtures.sourceOrderedOnly(1024, 100);

        assertThrows(
                IllegalArgumentException.class,
                () -> AdjacencyOrdering.of(sourceOnly, AdjListType.ordered_by_dest));
    }

    @Test
    public void refusesMissingMetadata() {
        assertThrows(
                NullPointerException.class,
                () -> AdjacencyOrdering.of(null, AdjListType.ordered_by_source));
        assertThrows(NullPointerException.class, () -> AdjacencyOrdering.of(edgeInfo, null));
    }
}

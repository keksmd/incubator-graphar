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
import static org.junit.Assert.assertTrue;

import java.net.URI;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.type.AdjListType;
import org.junit.Test;

public class OrderedAdjacencyResolverTest {
    private final EdgeInfo edgeInfo = EdgeInfoFixtures.personKnowsPerson(1024, 100, 50);
    private final OrderedAdjacencyResolver resolver =
            new OrderedAdjacencyResolver(edgeInfo, AdjListType.ordered_by_source);

    @Test
    public void locatesTheOffsetChunkAndPairIndexForAVertex() {
        OffsetLocation location = resolver.locate(297);

        assertEquals(297, location.vertexId());
        assertEquals(2, location.vertexChunkIndex());
        assertEquals(97, location.offsetIndex());
        assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/offset/chunk2"),
                location.offsetChunkUri());
    }

    @Test
    public void resolvesAnOffsetPairIntoTheEdgeChunksThatHoldTheRows() {
        ResolvedAdjacency resolved = resolver.resolve(297, 1008, 1061);

        assertEquals(1008, resolved.edgeRange().begin());
        assertEquals(1061, resolved.edgeRange().end());
        assertEquals(0, resolved.edgeChunks().begin());
        assertEquals(2, resolved.edgeChunks().end());
    }

    @Test
    public void readsTheSamePairOutOfACompleteOffsetChunk() {
        long[] offsets = new long[101];
        for (int index = 1; index <= 97; index++) {
            offsets[index] = 1008;
        }
        for (int index = 98; index < offsets.length; index++) {
            offsets[index] = 1061;
        }

        ResolvedAdjacency resolved = resolver.resolve(297, OffsetChunk.of(offsets));

        assertEquals(1008, resolved.edgeRange().begin());
        assertEquals(1061, resolved.edgeRange().end());
        assertEquals(2, resolved.offsetLocation().vertexChunkIndex());
    }

    @Test
    public void namesTheAdjacencyChunksOfTheOwningVertexPartition() {
        ResolvedAdjacency resolved = resolver.resolve(297, 1008, 1061);

        assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/part2/chunk0"),
                resolved.adjacencyChunkUri(0));
        assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/adj_list/part2/chunk1"),
                resolved.adjacencyChunkUri(1));
        assertEquals(
                URI.create("edge/person_knows_person/ordered_by_source/edge_count2"),
                resolved.edgeCountUri());
    }

    @Test
    public void refusesAnAdjacencyChunkOutsideTheResolvedRange() {
        ResolvedAdjacency resolved = resolver.resolve(297, 1008, 1061);

        assertThrows(IllegalArgumentException.class, () -> resolved.adjacencyChunkUri(2));
    }

    @Test
    public void addressesADestinationOrderedLayoutThroughItsOwnChunkSize() {
        OrderedAdjacencyResolver byDest =
                new OrderedAdjacencyResolver(edgeInfo, AdjListType.ordered_by_dest);

        OffsetLocation location = byDest.locate(297);

        assertEquals(5, location.vertexChunkIndex());
        assertEquals(47, location.offsetIndex());
        assertEquals(
                URI.create("edge/person_knows_person/ordered_by_dest/offset/chunk5"),
                location.offsetChunkUri());
    }

    @Test
    public void resolvesAVertexWithNoEdgesToAnEmptySelection() {
        ResolvedAdjacency resolved = resolver.resolve(297, 1008, 1008);

        assertTrue(resolved.edgeRange().isEmpty());
        assertTrue(resolved.edgeChunks().isEmpty());
        assertEquals(0, resolved.edgeChunks().begin());
    }

    @Test
    public void refusesMetadataItCannotAddress() {
        assertThrows(
                NullPointerException.class,
                () -> new OrderedAdjacencyResolver(null, AdjListType.ordered_by_source));
        assertThrows(
                IllegalArgumentException.class,
                () -> new OrderedAdjacencyResolver(edgeInfo, AdjListType.unordered_by_source));
        assertThrows(IllegalArgumentException.class, () -> resolver.resolve(297, 1061, 1008));
    }
}

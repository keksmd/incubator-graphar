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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Tests GraphAr half-open chunk and edge range arithmetic. */
public class ChunkRangeTest {
    @Test
    public void derivesChunkPositionsAndCountsWithoutOverflow() {
        assertEquals(2L, ChunkMath.chunkIndex(299L, 100L));
        assertEquals(99L, ChunkMath.offsetInChunk(299L, 100L));
        assertEquals(0L, ChunkMath.chunkCount(0L, 100L));
        assertEquals(3L, ChunkMath.chunkCount(201L, 100L));
        assertEquals(Long.MAX_VALUE, ChunkMath.chunkCount(Long.MAX_VALUE, 1L));
    }

    @Test
    public void choosesTheChunkContainingTheLastIncludedEdge() {
        EdgeRange range = EdgeRange.fromOffsets(1008L, 1061L);

        ChunkRange chunks = range.edgeChunks(1024L);

        assertEquals(0L, chunks.begin());
        assertEquals(2L, chunks.end());
        assertTrue(chunks.contains(0L));
        assertTrue(chunks.contains(1L));
        assertFalse(chunks.contains(2L));
    }

    @Test
    public void keepsEmptyRangesAtTheirBeginChunk() {
        ChunkRange chunks = EdgeRange.fromOffsets(2048L, 2048L).edgeChunks(1024L);

        assertEquals(2L, chunks.begin());
        assertEquals(2L, chunks.end());
        assertTrue(chunks.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNegativeElementIdentifiers() {
        ChunkMath.chunkIndex(-1L, 1L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNonPositiveChunkSizes() {
        EdgeRange.fromOffsets(0L, 1L).edgeChunks(0L);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsDecreasingOffsetPairs() {
        EdgeRange.fromOffsets(2L, 1L);
    }
}

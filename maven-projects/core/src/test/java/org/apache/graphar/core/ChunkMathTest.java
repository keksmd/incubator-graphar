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

import org.junit.Test;

public class ChunkMathTest {
    @Test
    public void placesAnElementInTheChunkThatHoldsIt() {
        assertEquals(0, ChunkMath.chunkIndex(0, 100));
        assertEquals(0, ChunkMath.chunkIndex(99, 100));
        assertEquals(1, ChunkMath.chunkIndex(100, 100));
        assertEquals(2, ChunkMath.chunkIndex(297, 100));
    }

    @Test
    public void reportsThePositionOfAnElementInsideItsChunk() {
        assertEquals(0, ChunkMath.offsetInChunk(0, 100));
        assertEquals(99, ChunkMath.offsetInChunk(99, 100));
        assertEquals(0, ChunkMath.offsetInChunk(100, 100));
        assertEquals(97, ChunkMath.offsetInChunk(297, 100));
    }

    @Test
    public void countsTheChunksAPartitionNeeds() {
        assertEquals(0, ChunkMath.chunkCount(0, 100));
        assertEquals(1, ChunkMath.chunkCount(1, 100));
        assertEquals(1, ChunkMath.chunkCount(100, 100));
        assertEquals(2, ChunkMath.chunkCount(101, 100));
    }

    @Test
    public void staysExactForCountsNearTheLongLimit() {
        assertEquals(Long.MAX_VALUE / 2, ChunkMath.chunkIndex(Long.MAX_VALUE - 1, 2));
        assertEquals(1 + (Long.MAX_VALUE - 1) / 2, ChunkMath.chunkCount(Long.MAX_VALUE, 2));
    }

    @Test
    public void refusesNegativeElementIdentifiers() {
        assertThrows(IllegalArgumentException.class, () -> ChunkMath.chunkIndex(-1, 100));
        assertThrows(IllegalArgumentException.class, () -> ChunkMath.offsetInChunk(-1, 100));
        assertThrows(IllegalArgumentException.class, () -> ChunkMath.chunkCount(-1, 100));
    }

    @Test
    public void refusesANonPositiveChunkSize() {
        assertThrows(IllegalArgumentException.class, () -> ChunkMath.chunkIndex(0, 0));
        assertThrows(IllegalArgumentException.class, () -> ChunkMath.offsetInChunk(0, -1));
        assertThrows(IllegalArgumentException.class, () -> ChunkMath.chunkCount(0, 0));
    }
}

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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class EdgeRangeTest {
    @Test
    public void describesTheRowsBetweenTwoOffsetValues() {
        EdgeRange range = EdgeRange.fromOffsets(1008, 1061);

        assertEquals(1008, range.begin());
        assertEquals(1061, range.end());
        assertEquals(53, range.length());
        assertFalse(range.isEmpty());
    }

    @Test
    public void selectsNoRowsWhenTheOffsetsRepeat() {
        EdgeRange range = EdgeRange.fromOffsets(7, 7);

        assertTrue(range.isEmpty());
        assertEquals(0, range.length());
    }

    @Test
    public void coversEveryEdgeChunkTheRowsTouch() {
        ChunkRange chunks = EdgeRange.fromOffsets(1008, 1061).edgeChunks(1024);

        assertEquals(0, chunks.begin());
        assertEquals(2, chunks.end());
    }

    @Test
    public void stopsAtTheChunkBoundaryWhenTheRangeEndsOnIt() {
        ChunkRange chunks = EdgeRange.fromOffsets(0, 1024).edgeChunks(1024);

        assertEquals(0, chunks.begin());
        assertEquals(1, chunks.end());
    }

    @Test
    public void keepsAnEmptyRangeAnchoredToItsOwnChunk() {
        ChunkRange chunks = EdgeRange.fromOffsets(2048, 2048).edgeChunks(1024);

        assertEquals(2, chunks.begin());
        assertTrue(chunks.isEmpty());
    }

    @Test
    public void refusesNegativeOrNonMonotonicOffsets() {
        assertThrows(IllegalArgumentException.class, () -> EdgeRange.fromOffsets(-1, 5));
        assertThrows(IllegalArgumentException.class, () -> EdgeRange.fromOffsets(5, 4));
        assertThrows(
                IllegalArgumentException.class, () -> EdgeRange.fromOffsets(0, 10).edgeChunks(0));
    }
}

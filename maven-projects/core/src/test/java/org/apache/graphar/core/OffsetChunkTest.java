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

public class OffsetChunkTest {
    @Test
    public void countsOneFewerVertexThanItHoldsValues() {
        OffsetChunk chunk = OffsetChunk.of(new long[] {0, 3, 3, 9});

        assertEquals(3, chunk.vertexCount());
    }

    @Test
    public void resolvesEachLocalVertexToItsAdjacentOffsetPair() {
        OffsetChunk chunk = OffsetChunk.of(new long[] {0, 3, 3, 9});

        assertEquals(0, chunk.rangeFor(0).begin());
        assertEquals(3, chunk.rangeFor(0).end());
        assertEquals(0, chunk.rangeFor(1).length());
        assertEquals(3, chunk.rangeFor(2).begin());
        assertEquals(9, chunk.rangeFor(2).end());
    }

    @Test
    public void keepsItsOwnCopyOfTheSuppliedValues() {
        long[] values = {0, 3, 9};
        OffsetChunk chunk = OffsetChunk.of(values);
        values[1] = 7;

        assertEquals(3, chunk.rangeFor(0).end());
    }

    @Test
    public void acceptsAFinalOffsetThatMatchesThePartitionEdgeCount() {
        OffsetChunk.of(new long[] {0, 3, 9}).validateEdgeCount(9);
    }

    @Test
    public void refusesAFinalOffsetThatDisagreesWithTheEdgeCount() {
        OffsetChunk chunk = OffsetChunk.of(new long[] {0, 3, 9});

        assertThrows(IllegalArgumentException.class, () -> chunk.validateEdgeCount(8));
        assertThrows(IllegalArgumentException.class, () -> chunk.validateEdgeCount(-1));
    }

    @Test
    public void refusesValuesThatCannotDescribeAnOrderedLayout() {
        assertThrows(NullPointerException.class, () -> OffsetChunk.of(null));
        assertThrows(IllegalArgumentException.class, () -> OffsetChunk.of(new long[] {0}));
        assertThrows(IllegalArgumentException.class, () -> OffsetChunk.of(new long[] {1, 4}));
        assertThrows(IllegalArgumentException.class, () -> OffsetChunk.of(new long[] {0, 4, 2}));
    }

    @Test
    public void refusesALocalVertexOutsideTheChunk() {
        OffsetChunk chunk = OffsetChunk.of(new long[] {0, 3, 9});

        assertThrows(IllegalArgumentException.class, () -> chunk.rangeFor(-1));
        assertThrows(IllegalArgumentException.class, () -> chunk.rangeFor(2));
    }
}

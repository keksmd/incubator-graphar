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

package org.apache.graphar.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class RowRangeTest {
    @Test
    public void keepsTheHalfOpenBoundsItWasGiven() {
        RowRange range = new RowRange(10, 25);

        assertEquals(10, range.startInclusive());
        assertEquals(25, range.endExclusive());
    }

    @Test
    public void acceptsARangeThatSelectsNoRows() {
        RowRange empty = new RowRange(7, 7);

        assertEquals(empty.startInclusive(), empty.endExclusive());
    }

    @Test
    public void comparesByBothBounds() {
        assertEquals(new RowRange(1, 4), new RowRange(1, 4));
        assertEquals(new RowRange(1, 4).hashCode(), new RowRange(1, 4).hashCode());
        assertNotEquals(new RowRange(1, 4), new RowRange(1, 5));
    }

    @Test
    public void refusesANegativeStartOrAnEndBeforeTheStart() {
        assertThrows(IllegalArgumentException.class, () -> new RowRange(-1, 5));
        assertThrows(IllegalArgumentException.class, () -> new RowRange(5, 4));
    }
}

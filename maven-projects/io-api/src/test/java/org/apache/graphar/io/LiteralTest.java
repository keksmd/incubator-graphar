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

import java.time.Instant;
import java.time.LocalDate;
import org.junit.Test;

public class LiteralTest {
    @Test
    public void carriesEverySupportedScalarUnchanged() {
        assertEquals(Boolean.TRUE, Literal.of(Boolean.TRUE).value());
        assertEquals((byte) 1, Literal.of((byte) 1).value());
        assertEquals((short) 2, Literal.of((short) 2).value());
        assertEquals(3, Literal.of(3).value());
        assertEquals(4L, Literal.of(4L).value());
        assertEquals(5.5f, Literal.of(5.5f).value());
        assertEquals(6.5d, Literal.of(6.5d).value());
        assertEquals("seven", Literal.of("seven").value());
        assertEquals(LocalDate.of(2024, 1, 31), Literal.of(LocalDate.of(2024, 1, 31)).value());
        assertEquals(Instant.ofEpochMilli(8), Literal.of(Instant.ofEpochMilli(8)).value());
    }

    @Test
    public void comparesByValue() {
        assertEquals(Literal.of(4L), Literal.of(4L));
        assertEquals(Literal.of(4L).hashCode(), Literal.of(4L).hashCode());
        assertNotEquals(Literal.of(4L), Literal.of(4));
        assertNotEquals(Literal.of(4L), "4");
    }

    @Test
    public void refusesAValueAFilterCannotComparePhysically() {
        assertThrows(NullPointerException.class, () -> Literal.of(null));
        assertThrows(IllegalArgumentException.class, () -> Literal.of(new byte[] {1}));
        assertThrows(IllegalArgumentException.class, () -> Literal.of(Float.NaN));
        assertThrows(IllegalArgumentException.class, () -> Literal.of(Double.POSITIVE_INFINITY));
    }

    @Test
    public void refusesATimestampFinerThanTheFormatCanStore() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Literal.of(Instant.ofEpochSecond(1, 1_500_000)));
        Literal.of(Instant.ofEpochSecond(1, 2_000_000));
    }
}

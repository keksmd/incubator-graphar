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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class ProjectionTest {
    @Test
    public void requestsEveryColumnWithoutNamingOne() {
        Projection projection = Projection.all();

        assertTrue(projection.isAllColumns());
        assertTrue(projection.columns().isEmpty());
    }

    @Test
    public void keepsTheRequestedColumnsInTheGivenOrder() {
        Projection projection = Projection.of(Arrays.asList("id", "name"));

        assertFalse(projection.isAllColumns());
        assertEquals(Arrays.asList("id", "name"), projection.columns());
    }

    @Test
    public void doesNotFollowLaterEditsToTheSuppliedList() {
        List<String> columns = new ArrayList<>(Arrays.asList("id", "name"));
        Projection projection = Projection.of(columns);
        columns.add("extra");

        assertEquals(2, projection.columns().size());
        assertThrows(
                UnsupportedOperationException.class, () -> projection.columns().add("injected"));
    }

    @Test
    public void comparesByRequestedColumnsAndOrder() {
        assertEquals(Projection.of(List.of("id")), Projection.of(List.of("id")));
        assertEquals(
                Projection.of(List.of("id")).hashCode(), Projection.of(List.of("id")).hashCode());
        assertNotEquals(Projection.of(List.of("id", "name")), Projection.of(List.of("name", "id")));
        assertNotEquals(Projection.all(), Projection.of(List.of("id")));
    }

    @Test
    public void refusesAProjectionThatSelectsNothingOrRepeatsAColumn() {
        assertThrows(IllegalArgumentException.class, () -> Projection.of(null));
        assertThrows(IllegalArgumentException.class, () -> Projection.of(List.of()));
        assertThrows(
                IllegalArgumentException.class, () -> Projection.of(Arrays.asList("id", "id")));
        assertThrows(IllegalArgumentException.class, () -> Projection.of(Arrays.asList("id", " ")));
        assertThrows(
                IllegalArgumentException.class, () -> Projection.of(Arrays.asList("id", null)));
    }
}

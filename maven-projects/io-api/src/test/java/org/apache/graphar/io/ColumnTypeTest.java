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
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import org.junit.Test;

public class ColumnTypeTest {

    @Test
    public void describesAScalarKindWithoutAnElementType() {
        ColumnType type = ColumnType.of(ColumnType.Kind.INT64);

        assertEquals(ColumnType.Kind.INT64, type.kind());
        assertEquals(Optional.empty(), type.elementType());
    }

    @Test
    public void describesANestedListThroughItsElementType() {
        ColumnType inner = ColumnType.of(ColumnType.Kind.STRING);
        ColumnType nested = ColumnType.listOf(ColumnType.listOf(inner));

        assertEquals(ColumnType.Kind.LIST, nested.kind());
        assertEquals(ColumnType.Kind.LIST, nested.elementType().orElseThrow().kind());
        assertEquals(inner, nested.elementType().orElseThrow().elementType().orElseThrow());
    }

    @Test
    public void refusesToBuildAListThroughTheScalarFactory() {
        IllegalArgumentException failure =
                assertThrows(
                        IllegalArgumentException.class, () -> ColumnType.of(ColumnType.Kind.LIST));

        assertTrue(failure.getMessage().contains("listOf"));
    }

    @Test
    public void refusesAMissingKindOrElementType() {
        assertThrows(NullPointerException.class, () -> ColumnType.of(null));
        assertThrows(NullPointerException.class, () -> ColumnType.listOf(null));
    }

    @Test
    public void comparesEqualOnlyWhenKindAndElementTypeMatch() {
        ColumnType listOfInt = ColumnType.listOf(ColumnType.of(ColumnType.Kind.INT32));
        ColumnType sameListOfInt = ColumnType.listOf(ColumnType.of(ColumnType.Kind.INT32));
        ColumnType listOfLong = ColumnType.listOf(ColumnType.of(ColumnType.Kind.INT64));

        assertEquals(listOfInt, sameListOfInt);
        assertEquals(listOfInt.hashCode(), sameListOfInt.hashCode());
        assertNotEquals(listOfInt, listOfLong);
        assertNotEquals(listOfInt, ColumnType.of(ColumnType.Kind.INT32));
    }
}

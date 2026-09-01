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

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
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

        assertTrue(failure.getMessage().contains("dedicated factory"));
    }

    @Test
    public void retainsNamesAndNullabilityInNestedShapes() {
        Field element = new Field("item", ColumnType.of(ColumnType.Kind.INT32), false);
        Field attribute = new Field("weight", ColumnType.of(ColumnType.Kind.FLOAT64), true);
        ColumnType type =
                ColumnType.structOf(
                        List.of(
                                new Field("ids", ColumnType.fixedSizeListOf(element, 3), false),
                                attribute));

        assertEquals(ColumnType.Kind.STRUCT, type.kind());
        assertEquals("ids", type.children().get(0).name());
        assertEquals(OptionalInt.of(3), type.children().get(0).type().fixedSize());
        assertEquals(element.type(), type.children().get(0).type().elementType().orElseThrow());
        assertTrue(attribute.nullable());
    }

    @Test
    public void modelsMapDecimalAndFixedWidthBinaryWithoutAmbiguousFactories() {
        Field key = new Field("key", ColumnType.of(ColumnType.Kind.STRING), false);
        Field value = new Field("value", ColumnType.decimal(12, 2), true);
        ColumnType map = ColumnType.mapOf(key, value);
        ColumnType binary = ColumnType.fixedSizeBinary(16);

        assertEquals(List.of(key, value), map.children());
        assertEquals(OptionalInt.of(12), value.type().precision());
        assertEquals(OptionalInt.of(2), value.type().scale());
        assertEquals(OptionalInt.of(16), binary.fixedSize());
        assertThrows(IllegalArgumentException.class, () -> ColumnType.mapOf(value, key));
        assertThrows(IllegalArgumentException.class, () -> ColumnType.decimal(0, 0));
        assertThrows(IllegalArgumentException.class, () -> ColumnType.fixedSizeBinary(0));
    }

    @Test
    public void refusesAMissingKindOrElementType() {
        assertThrows(NullPointerException.class, () -> ColumnType.of(null));
        assertThrows(NullPointerException.class, () -> ColumnType.listOf((ColumnType) null));
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
